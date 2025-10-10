package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.RecurringJobsResult;
import org.jobrunr.storage.sql.common.DefaultSqlStorageProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.jobrunr.jobs.states.StateName.ENQUEUED;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.jobs.states.StateName.SCHEDULED;

//This class is responsible for processing recurring jobs in a JobRunr server environment in the MASTER instance.
public class ProcessRecurringJobsTask extends AbstractJobZooKeeperTask {

    private Boolean amIMaster = false; // This is used to check if the current instance is the master instance, jobrunr knows that it is master, but in context of ProcessRecurringJobsTask, we don't
    private final Map<String, Instant> recurringJobRuns; // This is populated in registerRecurringJobRun
    private RecurringJobsResult recurringJobs; // This stores all the millions of jobs
    private Map<Long, Long> recurringJobHash; // This will store the epoch time of window start of X amount time, and the hash of the jobs in that window
    //If the window start time's hash is same as in memory, we don't need to fetch the jobs again

    //Here we fetch all the existingJobs in jobrunr_jobs to check if the job is already scheduled, enqueued or processing
    Map<String,Long> existingJobsById;
    
    private boolean hasLoggedHourlyAudit = false; // Flag to ensure hourly audit is logged only once on master boot
    private Integer currentHour = null; // Current UTC hour for filtering
    private Integer fetchedHour = null; // Last fetched UTC hour to detect hour changes


    public ProcessRecurringJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.recurringJobRuns = new HashMap<>();
        this.recurringJobHash = new HashMap<>();
        this.recurringJobs = new RecurringJobsResult();
    }

    @Override
    protected void runTask() {
        long taskStart = System.currentTimeMillis();
        LOGGER.info("[SCHEDULE JOBS]: Starting task to schedule recurring jobs.");        
        Instant initialRunStartTime = runStartTime();
        Instant from = initialRunStartTime;        
        Instant upUntil = runStartTime().plus(backgroundJobServerConfiguration().getPollInterval());

        // Check if the current instance is the master instance
        if (!this.amIMaster) {
            LOGGER.info("[SCHEDULE JOBS][FLUXCAPACITOR]: This instance was not the master instance. Looks like a crash happened. Let's go back in time.");
            Instant oneMinuteAgo = initialRunStartTime.minus(Duration.ofMinutes(1));
            Instant lastSuccess = storageProvider.getLastSucceedJobUpdateTime();
            LOGGER.info("[SCHEDULE JOBS][FLUXCAPACITOR]: Initial start time: " + initialRunStartTime);
            LOGGER.info("[SCHEDULE JOBS][FLUXCAPACITOR]: Last success time: " + lastSuccess);
            LOGGER.info("[SCHEDULE JOBS][FLUXCAPACITOR]: One minute ago: " + oneMinuteAgo);
            // Time travel to the last success time
            initialRunStartTime = oneMinuteAgo.isAfter(lastSuccess) ? oneMinuteAgo : lastSuccess;
            LOGGER.info("[SCHEDULE JOBS][FLUXCAPACITOR]: Time travelling to: " + initialRunStartTime);
            this.amIMaster = true; // set this instance as the master instance only in the context of ProcessRecurringJobsTask
        } 
        else {
            /*
            * We need to go back in time, at the start of every hour too, to prevent missing of jobs
            * This happens when poll happened at 7:59:59 and next poll was 8:00:30
            * We miss running all 8AM jobs
            * This regression behaviour was the result of the hourMask logic.
            * Another way to solve this problem is to fetch all 8AM jobs at 7:58 or so -- but it causes further regression. 
            */
            Integer currentHourForTimetravel = Instant.now().atZone(ZoneOffset.UTC).getHour();
            boolean needsRefreshForTimetravel = fetchedHour == null || !currentHourForTimetravel.equals(fetchedHour);
            if(needsRefreshForTimetravel){
                //Go back in time to the top of the hour and go back 5 more seocods.
                from = Instant.now().atZone(ZoneOffset.UTC)
                    .withMinute(0)
                    .withSecond(0)
                    .withNano(0)
                    .minusSeconds(5)  // Go back 5 more seconds to be safe. 
                    .toInstant();
                LOGGER.info("[SCHEDULE JOBS][HOUR CHANGE]: Hour changed to {}, adjusting the FROM time to: {}",
                  currentHourForTimetravel, from);
            }
        }

        List<RecurringJob> recurringJobs = getRecurringJobs(); //Main function to fetch the recurring jobs

        // Calculate current hour mask for consistent filtering
        int currentHour = Instant.now().atZone(ZoneOffset.UTC).getHour();
        long hourMask = calculateHourMask(currentHour);

        //This code fetches the existing jobs in the database which are already scheduled, enqueued or processing
        existingJobsById = fetchExistingCountsByHours(hourMask); //optimized for hour-based filtering

        final Instant finalFrom = from;                                                                                                                                            
        convertAndProcessManyJobs(recurringJobs,
                recurringJob -> toScheduledJobs(recurringJob, finalFrom, upUntil),
                totalAmountOfJobs -> LOGGER.debug("[SCHEDULE JOBS]: Found {} jobs to schedule from {} recurring jobs", totalAmountOfJobs, recurringJobs.size()));
        
        long taskEnd = System.currentTimeMillis();
        LOGGER.info("[SCHEDULE JOBS]: Completed task to schedule recurring jobs in {}ms", (taskEnd - taskStart));
    }


    private long calculateHash(List<RecurringJob> jobs) {
        return jobs.stream()
                .map(recurringJob -> recurringJob.getCreatedAt().toEpochMilli())
                .reduce(Long::sum)
                .orElse(0L);
    }

    /**
     * Calculates the hour mask for fetching recurring jobs.
     * Always fetches current hour AND previous hour to ensure we never miss jobs.
     * This handles:
     * - DST transitions (jobs shifting UTC hours due to timezone offset changes)
     * - Hour boundary edge cases (poll at 7:59:59, next at 8:00:30)
     * - Jobs scheduled in different timezones
     * Deduplication via toScheduleJobs() and recurringJobRuns prevents double-scheduling.
     *
     * @param currentHour The current UTC hour (0-23)
     * @return Bitmask with the current hour and previous hour
     */
    private long calculateHourMask(int currentHour) {
        // Always fetch current hour AND previous hour
        int previousHour = (currentHour - 1 + 24) % 24;
        long mask = (1L << currentHour) | (1L << previousHour);

        LOGGER.debug("[SCHEDULE JOBS][HOUR MASK]: Fetching jobs for hours {} and {} (mask={})",
                    previousHour, currentHour, mask);
        return mask;
    }

    private List<RecurringJob> getRecurringJobs() {
        // Calculate current UTC hour
        currentHour = Instant.now().atZone(ZoneOffset.UTC).getHour();
        
        // Check if this is bootup/crash (fetchedHour is null) or hour changed
        boolean needsRefresh = fetchedHour == null || !currentHour.equals(fetchedHour);

        if (recurringJobs == null || recurringJobs.isEmpty() || needsRefresh) {
            if (fetchedHour == null) {
                LOGGER.info("[SCHEDULE JOBS][BOOTUP]: Boot up time, fetching recurring jobs for UTC hour filtering.");
            } else {
                LOGGER.info("[SCHEDULE JOBS][BOOTUP][HOUR CHANGE]: UTC hour changed from {} to {}, refreshing job cache", fetchedHour, currentHour);
            }


            // Create hour bitmask (DST-aware during transition periods)
            long hourMask = calculateHourMask(currentHour);
            LOGGER.info("[SCHEDULE JOBS][BOOTUP][UTC_FILTERING]: Fetching jobs for hour mask {} (current UTC hour: {})",
                       hourMask, currentHour);

            long fetchStart = System.currentTimeMillis();
            this.recurringJobs = storageProvider.getRecurringJobsByHours(hourMask);
            long fetchEnd = System.currentTimeMillis();
            
            // Update fetchedHour to current hour after successful fetch
            fetchedHour = currentHour;
            
            LOGGER.info("[SCHEDULE JOBS][BOOTUP][UTC FILTERING]: Hour-filtered fetch duration: " + (fetchEnd - fetchStart) + "ms");
            LOGGER.info("[SCHEDULE JOBS][BOOTUP][UTC FILTERING]: Hour-filtered fetch size: " + recurringJobs.size());
            
            return recurringJobs;
        }


        // Calculate hour mask for current cache validation (DST-aware during transition periods)
        long hourMask = calculateHourMask(currentHour);

        //This logic checks if the recurring jobs have been updated in the database
        //If the hash of the jobs in the database is same as in memory, we don't need to fetch the jobs again
        if (!storageProvider.recurringJobsUpdatedByHours(recurringJobs.getLastModifiedHash(), hourMask)) {
            LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: Hour-filtered recurring jobs have not been updated in the database. We can use the cached jobs.");
            return recurringJobs;
        }


        /*
         If we are here, we need to fetch the jobs again, because the hash has changed in the db.
         By default, we do a simple full refresh with hour filtering (since hour filtering already
         dramatically reduces the dataset). Window-based fetching is overkill for most cases.
        */

        boolean useWindowFetch = System.getenv("JOBRUNR_FETCH_WINDOWS") != null
                                 ? Boolean.parseBoolean(System.getenv("JOBRUNR_FETCH_WINDOWS"))
                                 : false;

        if (!useWindowFetch) {
            // Default behavior: Simple full refresh with hour filtering
            LOGGER.info("[SCHEDULE JOBS][REFRESH][UTC_FILTERING]: Doing simple hour-filtered refresh for mask {} (current UTC hour: {})",
                       hourMask, currentHour);

            long fetchStart = System.currentTimeMillis();
            this.recurringJobs = storageProvider.getRecurringJobsByHours(hourMask);
            long fetchEnd = System.currentTimeMillis();

            LOGGER.info("[SCHEDULE JOBS][REFRESH][UTC FILTERING]: Hour-filtered refresh completed in {}ms, fetched {} jobs",
                       (fetchEnd - fetchStart), recurringJobs.size());

            return recurringJobs;
        }

        /*
         JOBRUNR_FETCH_WINDOWS is enabled: Use optimized window-based fetching.
         This fetches only the time windows that have changed, rather than all jobs.
         Only useful for extremely large datasets with millions of jobs.
        */

        LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: The hour-filtered recurring jobs have changed in the database. We need to fetch the hash windows again.");
        // This is a heavy operation, so we need to do it only if the hash has changed
        // We will fetch hash windows only for the smaller subset of jobs we determined with hourOfExecution column
        Instant recurringJobHashStart = Instant.now();
        this.recurringJobHash = storageProvider.getRecurringJobsHashByHours(hourMask);
        Instant recurringJobHashEnd = Instant.now();
        LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: Hour-filtered recurring job hash fetch duration: " + Duration.between(recurringJobHashStart, recurringJobHashEnd).toMillis() + "ms");

        // make a mutable copy and sort by createdAt ascending
        List<RecurringJob> mutable = new ArrayList<>(recurringJobs);
        // determine our paging window: from the earliest job we know about…
        long windowStart = mutable.get(0).getCreatedAt().toEpochMilli();
        // …up to now, in 12-hour increments
        long now      = System.currentTimeMillis();
        long interval = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

        /*
         * Now, we have the hash of the jobs in the database
         * We will now slice by time, and compare it with the hash of the jobs locally
         * If hash has changed, we will refetch the jobs
         */
        while (windowStart < now ) {
            long windowEnd = Math.min(windowStart + interval, now);
    
            // LOGGER.info("Page Window: " + windowStart + " to " + windowEnd);
            // pick out only the local jobs in this time slice
            // copy into final locals for the lambda
            final long ws = windowStart;
            final long we = windowEnd;

            List<RecurringJob> localSlice = mutable.stream()
                .filter(j -> { 
                    long ts = j.getCreatedAt().toEpochMilli();
                    return ts >= ws && ts < we;
                })
                .collect(Collectors.toList());
    
            long localHash = calculateHash(localSlice);
            //lookup the windowStart in recurringJobHash
            long dbHash = recurringJobHash.getOrDefault(windowStart, 0L);


            if (localHash != dbHash) {
                LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: 🚨 Hash mismatch at offset: " + windowStart + ". Will fetch fresh hour-filtered page.");
                // fetch only that N‑minute batch from database with hour filtering
                Instant fetchStart = Instant.now();
                List<RecurringJob> fresh = storageProvider.getRecurringJobsPageByHours(windowStart, windowEnd, hourMask);
                Instant fetchEnd = Instant.now();
                LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: Fresh hour-filtered page fetch duration: " + Duration.between(fetchStart, fetchEnd).toMillis() + "ms");
                LOGGER.info("[SCHEDULE JOBS][RECURRING JOB HASH]: Fresh hour-filtered page size: " + fresh.size());
                // replace in existing recurringJobs list
                // find the range in the current list that belongs to [ws,we)
                int startIdx = firstIndexOfTimestamp(mutable, windowStart);
                int endIdx   = firstIndexOfTimestamp(mutable, windowEnd);

                // remove the old slice
                for (int i = startIdx; i < endIdx; i++) {
                    mutable.remove(startIdx); // each removal shifts the rest left
                }
                // insert all the fresh jobs at startIdx
                mutable.addAll(startIdx, fresh);
            }
    
            windowStart = windowEnd;
        }
    
        this.recurringJobs = new RecurringJobsResult(mutable);
        return mutable;
    }
    
    /** 
     * Find the first index in list where createdAt ≥ target.
     * If all < target, returns list.size().
     */
    private int firstIndexOfTimestamp(List<RecurringJob> list, long target) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getCreatedAt().toEpochMilli() >= target) {
                return i;
            }
        }
        return list.size();
    }
    
    // private List<RecurringJob> getRecurringJobs() {
    //     if (storageProvider.recurringJobsUpdated(recurringJobs.getLastModifiedHash())) {
    //         this.recurringJobs = storageProvider.getRecurringJobs();
    //     }
    //     return this.recurringJobs;
    // }

    List<Job> toScheduledJobs(RecurringJob recurringJob, Instant from, Instant upUntil) {
        List<Job> jobsToSchedule = getJobsToSchedule(recurringJob, from, upUntil);
        if (jobsToSchedule.isEmpty()) {
            LOGGER.debug("[SCHEDULE JOBS][{}]: Recurring job {} resulted in 0 scheduled job.", recurringJob.getId(), recurringJob.getJobName());
        } else if (jobsToSchedule.size() > 1) {
            LOGGER.info("[SCHEDULE JOBS][{}]: Recurring job {} resulted in {} scheduled jobs. This means a long GC happened and JobRunr is catching up.", recurringJob.getId(), recurringJob.getJobName(), jobsToSchedule.size());
        } else if (isAlreadyScheduledEnqueuedOrProcessing(recurringJob)) {
            // if the job is already scheduled, enqueued or processing, we skip this run
            LOGGER.info("[SCHEDULE JOBS][{}]: Recurring job is already scheduled, enqueued or processing. Run will be skipped as job is taking longer than given CronExpression or Interval.", recurringJob.getId(), recurringJob.getJobName());
            jobsToSchedule.clear();
        } else if (jobsToSchedule.size() == 1) {
            // LOGGER.debug("[{}]: Recurring job {} resulted in 1 scheduled job.", recurringJob.getId(), recurringJob.getJobName());
        }
        registerRecurringJobRun(recurringJob, upUntil);
        return jobsToSchedule;
    }

    private List<Job> getJobsToSchedule(RecurringJob recurringJob, Instant runStartTime, Instant upUntil) {
        Instant lastRun = recurringJobRuns.getOrDefault(recurringJob.getId(), runStartTime);
        return recurringJob.toScheduledJobs(lastRun, upUntil);
    }

    private Map<String, Long> fetchExistingCounts() {
        Map<String, Long> existingJobsById = new HashMap<>();
        existingJobsById = storageProvider.recurringJobsExists(SCHEDULED, ENQUEUED, PROCESSING);
        return existingJobsById;
    }

    private Map<String, Long> fetchExistingCountsByHours(long hourMask) {
        Map<String, Long> existingJobsById = new HashMap<>();
        existingJobsById = storageProvider.recurringJobsExistsByHours(hourMask, SCHEDULED, ENQUEUED, PROCESSING);
        return existingJobsById;
    }

    // private boolean isAlreadyScheduledEnqueuedOrProcessing(RecurringJob recurringJob) {
    //     return storageProvider.recurringJobExists(recurringJob.getId(), SCHEDULED, ENQUEUED, PROCESSING);
    // }

    private boolean isAlreadyScheduledEnqueuedOrProcessing(RecurringJob recurringJob) {
        // true if we saw ≥1 existing job for this ID
        return existingJobsById.getOrDefault(recurringJob.getId(), 0L) > 0;
    }
    

    private void registerRecurringJobRun(RecurringJob recurringJob, Instant upUntil) {
        recurringJobRuns.put(recurringJob.getId(), upUntil);
    }
}