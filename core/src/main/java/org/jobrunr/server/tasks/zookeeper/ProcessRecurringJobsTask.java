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


    public ProcessRecurringJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.recurringJobRuns = new HashMap<>();
        this.recurringJobHash = new HashMap<>();
        this.recurringJobs = new RecurringJobsResult();
    }

    @Override
    protected void runTask() {
        LOGGER.trace("Looking for recurring jobs... ");

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
        
        //This code fetches the existing jobs in the database which are already scheduled, enqueued or processing
        existingById = fetchExistingCounts(); //a bit heavy

        convertAndProcessManyJobs(recurringJobs,
                recurringJob -> toScheduledJobs(recurringJob, from, upUntil),
                totalAmountOfJobs -> LOGGER.debug("Found {} jobs to schedule from {} recurring jobs", totalAmountOfJobs, recurringJobs.size()));
    }


    private long calculateHash(List<RecurringJob> jobs) {
        return jobs.stream()
                .map(recurringJob -> recurringJob.getCreatedAt().toEpochMilli())
                .reduce(Long::sum)
                .orElse(0L);
    }

    private List<RecurringJob> getRecurringJobs() {
        if (recurringJobs == null || recurringJobs.isEmpty()) {
            // first time, just fetch all from the database
            LOGGER.info("[BOOTUP]: Boot up time, fetching all recurring jobs.");
            long fetchStart = System.currentTimeMillis();
            this.recurringJobs = storageProvider.getRecurringJobs();
            long fetchEnd = System.currentTimeMillis();
            LOGGER.info("[BOOTUP]: First time fetch duration: " + (fetchEnd - fetchStart) + "ms");
            LOGGER.info("[BOOTUP]: First time fetch size: " + recurringJobs.size());
            return recurringJobs;
        }

        //This logic checks if the recurring jobs have been updated in the database
        //If the hash of the jobs in the database is same as in memory, we don't need to fetch the jobs again
        if (!storageProvider.recurringJobsUpdated(recurringJobs.getLastModifiedHash())) {
            LOGGER.info("[RECURRINGJOBHASH]: Recurring jobs have not been updated in the database. We can use the cached jobs.");
            return recurringJobs;
        }
    
        /*
         If we are here, we need to fetch the jobs again, becuase the hash has changed of the db
         But we don't need to fetch all the jobs again, we can just fetch the jobs that are in the time window
         We do need to fetch the hashes of the windows again.
        */

        LOGGER.info("[RECURRINGJOBHASH]: The recurring jobs have changed in the database. We need to fetch the hash windows again.");
        // This is a heavy operation, so we need to do it only if the hash has changed
        // We need to fetch the hash of the recurring jobs again at all poll intervals only if full db hash changes, can we optimize this?
        Instant recurringJobHashStart = Instant.now();
        this.recurringJobHash = storageProvider.getRecurringJobsHash();
        Instant recurringJobHashEnd = Instant.now();
        LOGGER.info("[RECURRINGJOBHASH]: Recurring job hash fetch duration: " + Duration.between(recurringJobHashStart, recurringJobHashEnd).toMillis() + "ms");

        // make a mutable copy and sort by createdAt ascending
        List<RecurringJob> mutable = new ArrayList<>(recurringJobs);
        // determine our paging window: from the earliest job we know about…
        long windowStart = mutable.get(0).getCreatedAt().toEpochMilli();
        // …up to now, in 12-hour increments
        long now      = System.currentTimeMillis();
        long interval = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

        // I'm not sure if this is the best way to do this, unable to check if it works, but check once
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
            // The below line is commented out because we are not using the database to calculate the hash
            // long dbHash    = storageProvider.recurringJobsUpdatedHash(windowStart, windowEnd);

            //Remove double if checks
            if (localHash != dbHash) {
                LOGGER.info("[RECURRINGJOBHASH]: 🚨 Hash mismatch at offset: " + windowStart + ". Will fetch fresh page.");
            }

            if (localHash != dbHash) {
                // fetch only that N‑minute batch from database
                Instant fetchStart = Instant.now();
                List<RecurringJob> fresh = storageProvider.getRecurringJobsPage(windowStart, windowEnd);
                Instant fetchEnd = Instant.now();
                LOGGER.info("[RECURRINGJOBHASH]: Fresh page fetch duration: " + Duration.between(fetchStart, fetchEnd).toMillis() + "ms");
                LOGGER.info("[RECURRINGJOBHASH]: Fresh page size: " + fresh.size());
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
}
