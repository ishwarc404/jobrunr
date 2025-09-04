package org.jobrunr.server;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.tasks.steward.OnboardNewWorkTask;
import org.jobrunr.server.tasks.steward.UpdateJobsInProgressTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The JobSteward manages everything related to local jobs (e.g. updating them periodically and fetching new work)
 */
public class JobSteward extends JobHandler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSteward.class);
    
    private final Map<Job, Thread> jobsCurrentlyInProgress;
    private final AtomicInteger occupiedWorkers;
    private final OnboardNewWorkTask onboardNewWorkTask;

    public JobSteward(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer,
                new UpdateJobsInProgressTask(backgroundJobServer),
                new OnboardNewWorkTask(backgroundJobServer));
        this.jobsCurrentlyInProgress = new ConcurrentHashMap<>();
        this.occupiedWorkers = new AtomicInteger();
        this.onboardNewWorkTask = getTaskOfType(OnboardNewWorkTask.class);
    }

    public void startProcessing(Job job, Thread thread) {
        Optional<Job> optionalExistingThatMayBeReplacedJob = jobsCurrentlyInProgress.keySet().stream().filter(j -> j.getId().equals(job.getId())).findFirst();
        optionalExistingThatMayBeReplacedJob
                .map(j -> j.delete("Job has been replaced"))
                .map(jobsCurrentlyInProgress::get)
                .ifPresent(Thread::interrupt);
        jobsCurrentlyInProgress.put(job, thread);
        // LOGGER.info("[JOB STEWARD]: Added job to tracking. Job ID: [{}] [recurringJobId:{}] [jobName:{}]", 
        //            job.getId(), job.getRecurringJobId().orElse(null), job.getJobName());
    }

    public void stopProcessing(Job job) {
        jobsCurrentlyInProgress.remove(job);
        // LOGGER.info("[JOB STEWARD]: Removed job from tracking. Job ID: [{}] [recurringJobId:{}] [jobName:{}]", 
        //            job.getId(), job.getRecurringJobId().orElse(null), job.getJobName());
    }

    public Set<Job> getJobsInProgress() {
        return jobsCurrentlyInProgress.keySet();
    }

    public Thread getThreadProcessingJob(Job job) {
        return jobsCurrentlyInProgress.get(job);
    }

    public int getOccupiedWorkerCount() {
        return occupiedWorkers.get();
    }

    public void notifyThreadOccupied() {
        occupiedWorkers.incrementAndGet();
    }

    /*
     *  Problem:
        When a JobRunr server is designated as master, it should only schedule jobs
        but not process them. However, the OnboardNewWorkTask was still being
        triggered by worker thread callbacks in notifyThreadIdle(), causing the
        master server to process jobs even after stopJobSteward() was called.

        This typically occurs when:
        1. Server starts up and begins processing jobs as a worker
        2. Server becomes master and stopJobSteward() is called
        3. Existing worker threads complete their jobs and call notifyThreadIdle()
        4. notifyThreadIdle() triggers onboardNewWorkTask.runTaskThreadSafe()
        5. Master server incorrectly processes new jobs

        Solution:
        Added master check in JobSteward.notifyThreadIdle() to prevent the
        OnboardNewWorkTask from running when the server is in master mode.
        This ensures proper separation between master (scheduling) and worker
        (processing) responsibilities.
    */
    public void notifyThreadIdle() {
        this.occupiedWorkers.decrementAndGet();
        if(!getBackgroundJobServer().isMaster()){
            onboardNewWorkTask.runTaskThreadSafe();
        }
    }
}