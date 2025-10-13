package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import static java.util.Collections.emptyList;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt;

public class ProcessOrphanedJobsTask extends AbstractJobZooKeeperTask {

    private final int pageRequestSize;
    private final Duration serverTimeoutDuration;

    public ProcessOrphanedJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.pageRequestSize = backgroundJobServer.getConfiguration().getOrphanedJobsRequestSize();
        // this.serverTimeoutDuration = backgroundJobServer.getConfiguration().getPollInterval().multipliedBy(backgroundJobServer.getConfiguration().getServerTimeoutPollIntervalMultiplicand());
        /*
         * Updating this to handle long running jobs.
         * Orphan jobs = jobs which were spawned by servers who are now dead. If we have a really large value we will not know if a server is dead.
         * We need to have a very short value ideally like a few minutes at max
         */
        
        this.serverTimeoutDuration = Duration.ofMinutes(
            Optional.ofNullable(System.getenv("JOBRUNR_ORPHAN_TIMEOUT"))
                .map(timeout -> {
                    try {
                        int minutes = Integer.parseInt(timeout);
                        return Math.max(1, Math.min(minutes, 10)); // Clamp between 1-10 minutes
                    } catch (NumberFormatException e) {
                        return 2; //Default to 2
                    }
                })
                .orElse(2)
        );
    }

    @Override
    protected void runTask() {
        LOGGER.debug("[ORPHAN JOB]: Looking for orphan jobs... ");
        final Instant updatedBefore = runStartTime().minus(serverTimeoutDuration);
        processManyJobs(previousResults -> getOrphanedJobs(updatedBefore, previousResults),
                this::changeJobStateToFailedAndRunJobFilter,
                totalAmountOfOrphanedJobs -> LOGGER.debug("Found {} orphan jobs.", totalAmountOfOrphanedJobs));
    }

    private List<Job> getOrphanedJobs(Instant updatedBefore, List<Job> previousResults) {
        if (previousResults != null && previousResults.size() < pageRequestSize) return emptyList();
        return storageProvider.getJobList(PROCESSING, updatedBefore, ascOnUpdatedAt(pageRequestSize));
    }

    private void changeJobStateToFailedAndRunJobFilter(Job job) {
        LOGGER.warn("[ORPHAN JOB DETECTED]: [id:{}] [recurringJobId:{}] [jobName:{}] [lastUpdated:{}] - Job was too long in PROCESSING state without being updated (timeout: {})",
                job.getId(),
                job.getRecurringJobId().orElse(null),
                job.getJobName(),
                job.getUpdatedAt(),
                serverTimeoutDuration);

        IllegalThreadStateException e = new IllegalThreadStateException("Job was too long in PROCESSING state without being updated.");
        jobFilterUtils.runOnJobProcessingFailedFilters(job, e);

        // Create a lightweight exception without stack trace for storage
        IllegalThreadStateException lightweightException = new IllegalThreadStateException("Job was too long in PROCESSING state without being updated.");
        lightweightException.setStackTrace(new StackTraceElement[0]); // Empty stack trace to save DB space
        job.failed("Orphaned job", lightweightException);
    }
}
