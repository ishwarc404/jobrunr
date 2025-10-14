package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.utils.mapper.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LongRunningJobsTask extends AbstractJobZooKeeperTask {

    private final Duration longRunningThreshold;
    private final Duration recentlyUpdatedWindow;
    private final JsonMapper jsonMapper;

    public LongRunningJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.jsonMapper = backgroundJobServer.getJsonMapper();

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

        // Summary log
        logSummary(longRunningJobs);

        // JSON structured log
        logJsonSummary(longRunningJobs);
    }

    private void logSummary(List<Job> longRunningJobs) {
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("[LONG RUNNING JOB SUMMARY]: Found %d jobs exceeding %dm threshold | ",
            longRunningJobs.size(),
            longRunningThreshold.toMinutes()));

        LOGGER.warn(summary.toString());
    }

    private void logJsonSummary(List<Job> longRunningJobs) {
        LongRunningJobsReport report = new LongRunningJobsReport();
        report.count = longRunningJobs.size();
        report.thresholdMinutes = longRunningThreshold.toMinutes();
        report.jobs = new ArrayList<>();

        for (Job job : longRunningJobs) {
            Duration jobDuration = Duration.between(job.getCreatedAt(), job.getUpdatedAt());

            JobInfo jobInfo = new JobInfo();
            jobInfo.id = job.getId().toString();
            jobInfo.recurringJobId = job.getRecurringJobId().orElse(null);
            jobInfo.jobName = job.getJobName();
            jobInfo.state = job.getState().toString();
            jobInfo.durationMinutes = jobDuration.toMinutes();
            jobInfo.createdAt = job.getCreatedAt().toString();
            jobInfo.updatedAt = job.getUpdatedAt().toString();

            report.jobs.add(jobInfo);
        }

        String json = jsonMapper.serialize(report);
        LOGGER.warn("[LONG RUNNING JOB JSON]: {}", json);
    }

    // DTOs for JSON serialization
    private static class LongRunningJobsReport {
        public int count;
        public long thresholdMinutes;
        public List<JobInfo> jobs;
    }

    private static class JobInfo {
        public String id;
        public String recurringJobId;
        public String jobName;
        public String state;
        public long durationMinutes;
        public String createdAt;
        public String updatedAt;
    }
}
