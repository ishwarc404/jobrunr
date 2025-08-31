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
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.jobrunr.jobs.states.StateName.ENQUEUED;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.jobs.states.StateName.SCHEDULED;

//This class is responsible for processing recurring jobs in a JobRunr server environment in the MASTER instance.
public class ProcessRecurringJobsTask extends AbstractJobZooKeeperTask {

    private Boolean amIMaster = false; // This is used to check if the current instance is the master instance, jobrunr knows that it is master, but in context of ProcessRecurringJobsTask, we don't
    private final Map<String, Instant> recurringJobRuns; 
    private RecurringJobsResult recurringJobs; // This stores all the millions of jobs
    private Map<Long, Long> recurringJobHash; // This will store the epoch time of window start of X amount time, and the hash of the jobs in that window
    //If the window start time's hash is same as in memory, we don't need to fetch the jobs again

    //Here we fetch all the existingJobs in jobrunr_jobs to check if the job is already scheduled, enqueued or processing
    Map<String,Long> existingById;
    
    private boolean hasLoggedHourlyAudit = false; // Flag to ensure hourly audit is logged only once on master boot
    private Integer currentHour = null; // Current UTC hour for filtering
    private Integer fetchedHour = null; // Last fetched UTC hour to detect hour changes


    public ProcessRecurringJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        LOGGER.info("ProcessRecurringJobsTask starting.... ");
        this.recurringJobRuns = new HashMap<>();
        this.recurringJobHash = new HashMap<>();
        this.recurringJobs = new RecurringJobsResult();
    }

    @Override
    protected void runTask() {
        long taskStart = System.currentTimeMillis();
        LOGGER.info("[PROCESS RECURRING JOBS]: Starting runTask...");        
        Instant initialRunStartTime = runStartTime();

        // Check if the current instance is the master instance
        if (!this.amIMaster) {
            LOGGER.info("[FLUXCAPACITOR]: This instance was not the master instance. Looks like a crash happened. Let's go back in time.");
            Instant oneMinuteAgo = initialRunStartTime.minus(Duration.ofMinutes(1));
            Instant lastSuccess = storageProvider.getLastSucceedJobUpdateTime();
            LOGGER.info("[FLUXCAPACITOR]: Initial start time: " + initialRunStartTime);
            LOGGER.info("[FLUXCAPACITOR]: Last success time: " + lastSuccess);
            LOGGER.info("[FLUXCAPACITOR]: One minute ago: " + oneMinuteAgo);
            // Time travel to the last success time
            initialRunStartTime = oneMinuteAgo.isAfter(lastSuccess) ? oneMinuteAgo : lastSuccess;
            LOGGER.info("[FLUXCAPACITOR]: Time travelling to: " + initialRunStartTime);
            this.amIMaster = true; // set this instance as the master instance only in the context of ProcessRecurringJobsTask
        }

        Instant from = initialRunStartTime;        
        Instant upUntil = runStartTime().plus(backgroundJobServerConfiguration().getPollInterval());

        List<RecurringJob> recurringJobs = getRecurringJobs(); //Main function to fetch the recurring jobs
        
        // Calculate current hour mask for consistent filtering  
        int currentHour = Instant.now().atZone(ZoneOffset.UTC).getHour();
        long hourMask = (1L << currentHour);
        
        //This code fetches the existing jobs in the database which are already scheduled, enqueued or processing
        existingById = fetchExistingCountsByHours(hourMask); //optimized for hour-based filtering

        convertAndProcessManyJobs(recurringJobs,
                recurringJob -> toScheduledJobs(recurringJob, from, upUntil),
                totalAmountOfJobs -> LOGGER.debug("Found {} jobs to schedule from {} recurring jobs", totalAmountOfJobs, recurringJobs.size()));
        
        long taskEnd = System.currentTimeMillis();
        LOGGER.info("[PROCESS RECURRING JOBS]: Completed runTask in {}ms", (taskEnd - taskStart));
    }


    private long calculateHash(List<RecurringJob> jobs) {
        return jobs.stream()
                .map(recurringJob -> recurringJob.getCreatedAt().toEpochMilli())
                .reduce(Long::sum)
                .orElse(0L);
    }

    private List<RecurringJob> getRecurringJobs() {
        // Calculate current UTC hour
        currentHour = Instant.now().atZone(ZoneOffset.UTC).getHour();
        
        // Check if this is bootup/crash (fetchedHour is null) or hour changed
        boolean needsRefresh = fetchedHour == null || !currentHour.equals(fetchedHour);
        
        if (recurringJobs == null || recurringJobs.isEmpty() || needsRefresh) {
            if (fetchedHour == null) {
                LOGGER.info("[BOOTUP]: Boot up time, fetching recurring jobs for UTC hour filtering...");
            } else {
                LOGGER.info("[BOOTUP][HOUR_CHANGE]: UTC hour changed from {} to {}, refreshing job cache", fetchedHour, currentHour);
            }
            
        
            // Create hour bitmask for current hour
            long hourMask = (1L << currentHour); 
            
            LOGGER.info("[BOOTUP][UTC_FILTERING]: Fetching jobs for UTC hours {} (mask={})", 
                       currentHour, hourMask);
            
            long fetchStart = System.currentTimeMillis();
            this.recurringJobs = storageProvider.getRecurringJobsByHours(hourMask);
            long fetchEnd = System.currentTimeMillis();
            
            // Update fetchedHour to current hour after successful fetch
            fetchedHour = currentHour;
            
            LOGGER.info("[BOOTUP][UTC_FILTERING]: Hour-filtered fetch duration: " + (fetchEnd - fetchStart) + "ms");
            LOGGER.info("[BOOTUP][UTC_FILTERING]: Hour-filtered fetch size: " + recurringJobs.size());
            
            return recurringJobs;
        }

        /*
         * TODO - 
         * getLastModifiedHash
         * recurringJobsUpdated --- NEEDS TO FETCH and CHECK HASH OF ONLY hourOfExecution Jobs
         * getRecurringJobsHash --- NEEDS TO FETCH HASH OF ONLY hourOfExecution Jobs
         * getRecurringJobsPage --- NEEDS TO FETCH ONLY hourOfExecution Jobs
         */
        // Calculate hour mask for current cache validation
        long hourMask = (1L << currentHour);
        
        //This logic checks if the recurring jobs have been updated in the database
        //If the hash of the jobs in the database is same as in memory, we don't need to fetch the jobs again
        if (!storageProvider.recurringJobsUpdatedByHours(recurringJobs.getLastModifiedHash(), hourMask)) {
            LOGGER.info("[RECURRINGJOBHASH]: Hour-filtered recurring jobs have not been updated in the database. We can use the cached jobs.");
            return recurringJobs;
        }
    
        /*
         If we are here, we need to fetch the jobs again, becuase the hash has changed of the db
         But we don't need to fetch all the jobs again, we can just fetch the jobs that are in the time window
         We do need to fetch the hashes of the windows again.
        */

        LOGGER.info("[RECURRINGJOBHASH]: The hour-filtered recurring jobs have changed in the database. We need to fetch the hash windows again.");
        // This is a heavy operation, so we need to do it only if the hash has changed
        // We will fetch hash windows only for the smaller subset of jobs we determined with hourOfExecution column
        Instant recurringJobHashStart = Instant.now();
        this.recurringJobHash = storageProvider.getRecurringJobsHashByHours(hourMask);
        Instant recurringJobHashEnd = Instant.now();
        LOGGER.info("[RECURRINGJOBHASH]: Hour-filtered recurring job hash fetch duration: " + Duration.between(recurringJobHashStart, recurringJobHashEnd).toMillis() + "ms");

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
                LOGGER.info("[RECURRINGJOBHASH]: 🚨 Hash mismatch at offset: " + windowStart + ". Will fetch fresh hour-filtered page.");
                // fetch only that N‑minute batch from database with hour filtering
                Instant fetchStart = Instant.now();
                List<RecurringJob> fresh = storageProvider.getRecurringJobsPageByHours(windowStart, windowEnd, hourMask);
                Instant fetchEnd = Instant.now();
                LOGGER.info("[RECURRINGJOBHASH]: Fresh hour-filtered page fetch duration: " + Duration.between(fetchStart, fetchEnd).toMillis() + "ms");
                LOGGER.info("[RECURRINGJOBHASH]: Fresh hour-filtered page size: " + fresh.size());
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
            LOGGER.trace("[{}]: Recurring job {} resulted in 0 scheduled job.", recurringJob.getId(), recurringJob.getJobName());
        } else if (jobsToSchedule.size() > 1) {
            LOGGER.info("[{}]: Recurring job {} resulted in {} scheduled jobs. This means a long GC happened and JobRunr is catching up.", recurringJob.getId(), recurringJob.getJobName(), jobsToSchedule.size());
        } else if (isAlreadyScheduledEnqueuedOrProcessing(recurringJob)) {
            // if the job is already scheduled, enqueued or processing, we skip this run
            LOGGER.info("[{}]: Recurring job is already scheduled, enqueued or processing. Run will be skipped as job is taking longer than given CronExpression or Interval.", recurringJob.getId(), recurringJob.getJobName());
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
        Map<String, Long> existingById = new HashMap<>();
        existingById = storageProvider.recurringJobsExists(SCHEDULED, ENQUEUED, PROCESSING);
        return existingById;
    }

    private Map<String, Long> fetchExistingCountsByHours(long hourMask) {
        Map<String, Long> existingById = new HashMap<>();
        existingById = storageProvider.recurringJobsExistsByHours(hourMask, SCHEDULED, ENQUEUED, PROCESSING);
        return existingById;
    }

    // private boolean isAlreadyScheduledEnqueuedOrProcessing(RecurringJob recurringJob) {
    //     return storageProvider.recurringJobExists(recurringJob.getId(), SCHEDULED, ENQUEUED, PROCESSING);
    // }

    private boolean isAlreadyScheduledEnqueuedOrProcessing(RecurringJob recurringJob) {
        // true if we saw ≥1 existing job for this ID
        return existingById.getOrDefault(recurringJob.getId(), 0L) > 0;
    }
    

    private void registerRecurringJobRun(RecurringJob recurringJob, Instant upUntil) {
        recurringJobRuns.put(recurringJob.getId(), upUntil);
    }
    


    //Audit function
    // private void logHourlyJobDistribution() {
    //     try {
    //         List<RecurringJob> jobs = getRecurringJobs();
    //         int[] hourCounts = new int[24];
            
    //         for (RecurringJob job : jobs) {
    //             try {
    //                 String cronExpression = job.getScheduleExpression();
    //                 if (cronExpression != null) {
    //                     int[] hours = parseHoursFromCron(cronExpression);
    //                     for (int hour : hours) {
    //                         if (hour >= 0 && hour < 24) {
    //                             hourCounts[hour]++;
    //                         }
    //                     }
    //                 }
    //             } catch (Exception e) {
    //                 LOGGER.warn("Failed to parse cron for job: " + job.getId(), e);
    //             }
    //         }
            
    //         // Log the distribution
    //         StringBuilder auditLog = new StringBuilder("[AUDIT]: Hourly job distribution - [");
    //         for (int i = 0; i < 24; i++) {
    //             if (i > 0) auditLog.append(", ");
    //             auditLog.append(i).append(":").append(hourCounts[i]);
    //         }
    //         auditLog.append("]");
    //         LOGGER.info(auditLog.toString());
            
    //     } catch (Exception e) {
    //         LOGGER.error("Failed to log hourly job distribution audit", e);
    //     }
    // }

    // private int[] parseHoursFromCron(String cronExpression) {
    //     try {
    //         String[] parts = cronExpression.trim().split("\\s+");
            
    //         // Determine if this is 5-field or 6-field cron and get hour field
    //         String hourPart;
    //         if (parts.length == 5) {
    //             // Standard 5-field cron: minute hour day-of-month month day-of-week
    //             hourPart = parts[1];
    //         } else if (parts.length == 6) {
    //             // 6-field cron with seconds: second minute hour day-of-month month day-of-week
    //             hourPart = parts[2];
    //         } else {
    //             return new int[0]; // Return empty array for invalid cron
    //         }
            
    //         if ("*".equals(hourPart)) {
    //             return new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
    //         } else if (hourPart.contains(",")) {
    //             String[] hours = hourPart.split(",");
    //             return Arrays.stream(hours).mapToInt(h -> Integer.parseInt(h.trim())).toArray();
    //         } else {
    //             return new int[]{Integer.parseInt(hourPart)};
    //         }
    //     } catch (Exception e) {
    //         return new int[0]; // Return empty array for parsing failures
    //     }
    // }
}





/*  BATCH FETCH
    if (totalJobs > 1000_000) {
        // Use batched loading for large datasets
        LOGGER.info("[BOOTUP]: Large dataset detected (" + totalJobs + " jobs). Using batched loading with 1M batch size.");
        List<RecurringJob> allJobs = new ArrayList<>();
        int batchSize = 1000_000;
        long offset = 0;
        int batchNumber = 1;
        long totalFetchTime = 0;
        long totalJobsLoaded = 0;
        
        while (offset < totalJobs) {
            long batchStart = System.currentTimeMillis();
            List<RecurringJob> batch = storageProvider.getRecurringJobsBatch(offset, batchSize);
            long batchEnd = System.currentTimeMillis();
            long batchDuration = batchEnd - batchStart;
            totalFetchTime += batchDuration;
            totalJobsLoaded += batch.size();
            
            allJobs.addAll(batch);
            
            // Calculate progress percentage
            double progressPercent = Math.min(100.0, (double) totalJobsLoaded / totalJobs * 100.0);
            
            LOGGER.info("[BOOTUP]: Batch " + batchNumber + " - offset=" + offset + 
                    " fetched=" + batch.size() + " jobs in " + batchDuration + "ms" +
                    " | Progress: " + String.format("%.1f", progressPercent) + "% (" + 
                    totalJobsLoaded + "/" + totalJobs + ")");
            
            offset += batchSize;
            batchNumber++;
        }
        
        this.recurringJobs = new RecurringJobsResult(allJobs);
        LOGGER.info("[BOOTUP]: Batched loading completed. Total jobs loaded: " + allJobs.size() + 
                " in " + totalFetchTime + "ms across " + (batchNumber - 1) + " batches (100.0%)");
    } 
*/