package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class LongRunningJobsTask extends AbstractJobZooKeeperTask {

    private final Duration longRunningThreshold;
    private final Duration recentlyUpdatedWindow;

    public LongRunningJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);

        // Threshold for considering a job as long-running (default: 5 minutes)
        this.longRunningThreshold = Duration.ofMinutes(
            Optional.ofNullable(System.getenv("JOBRUNR_LONG_RUNNING_JOB_THRESHOLD"))
                .map(threshold -> {
                    try {
                        int minutes = Integer.parseInt(threshold);
                        return Math.max(1, minutes); // At least 1 minute
                    } catch (NumberFormatException e) {
                        return 5; // Default to 5 minutes
                    }
                })
                .orElse(5)
        );

        // Only consider jobs that are still actively being updated (within 2x poll interval)
        this.recentlyUpdatedWindow = backgroundJobServer.getConfiguration().getPollInterval().multipliedBy(2);
    }

    @Override
    protected void runTask() {
        LOGGER.debug("[LONG RUNNING JOB]: Looking for long-running jobs...");

        //We do this so that we only get jobs which are not orphaned.
        Instant updatedAfter = Instant.now().minus(recentlyUpdatedWindow);
        List<Job> longRunningJobs = storageProvider.getLongRunningJobs(longRunningThreshold, updatedAfter);

        if (longRunningJobs.isEmpty()) {
            LOGGER.debug("[LONG RUNNING JOB]: No long-running jobs found");
            return;
        }

        LOGGER.info("[LONG RUNNING JOB]: Found {} long-running jobs", longRunningJobs.size());

        for (Job job : longRunningJobs) {
            logLongRunningJob(job);
        }
    }

    private void logLongRunningJob(Job job) {
        Duration jobDuration = Duration.between(job.getCreatedAt(), job.getUpdatedAt());
        long durationMinutes = jobDuration.toMinutes();

        LOGGER.info("[LONG RUNNING JOB] [id:{}] [recurringJobId:{}] [jobName:{}] [state:{}] [duration:{}m] Job has been running for longer than threshold ({}m)",
                job.getId(),
                job.getRecurringJobId().orElse(null),
                job.getJobName(),
                job.getState(),
                durationMinutes,
                longRunningThreshold.toMinutes());
    }
}
