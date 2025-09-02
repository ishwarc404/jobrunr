package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;

import java.time.Instant;
import java.util.List;

import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt;

public class ProcessScheduledJobsTask extends AbstractJobZooKeeperTask {

    private final int pageRequestSize;

    public ProcessScheduledJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.pageRequestSize = backgroundJobServer.getConfiguration().getScheduledJobsRequestSize();
    }

    @Override
    protected void runTask() {
        long taskStart = System.currentTimeMillis();
        LOGGER.info("[ENQUEUE JOBS]: Looking for scheduled jobs to enqueue.");
        Instant scheduledBefore = now().plus(backgroundJobServerConfiguration().getPollInterval());
        processManyJobs(
            previousResults -> {
                List<Job> jobs = getJobsToSchedule(scheduledBefore, previousResults);
                return jobs;
            },
            Job::enqueue,
            totalAmountOfEnqueuedJobs -> LOGGER.debug("[ENQUE JOBS]: Found {} scheduled jobs to enqueue.", totalAmountOfEnqueuedJobs));
        long taskEnd = System.currentTimeMillis();
        LOGGER.info("[ENQUEUE JOBS]: Completed task to enqueue scheduled jobs in {}ms", (taskEnd - taskStart));
    }

    private List<Job> getJobsToSchedule(Instant scheduledBefore, List<Job> previousResults) {
        if (previousResults != null && previousResults.size() < pageRequestSize) return emptyList();
        return storageProvider.getScheduledJobs(scheduledBefore, ascOnUpdatedAt(pageRequestSize));
    }
}