package org.jobrunr.server.tasks.steward;

import java.util.Set;
import java.util.UUID;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpdateJobsInProgressTask extends AbstractJobStewardTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateJobsInProgressTask.class);

    public 
    UpdateJobsInProgressTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
    }

    @Override
    protected void runTask() {
        Set<Job> jobsInProgress = backgroundJobServer.getJobSteward().getJobsInProgress();
        UUID serverId = backgroundJobServer.getId();

        if (jobsInProgress.isEmpty()) {
            LOGGER.debug("[UPDATE JOBS IN PROGRESS] [serverId:{}] No jobs currently in progress to update", serverId);
            return;
        }
        convertAndProcessJobs(jobsInProgress, this::updateCurrentlyProcessingJob);
    }

    private Job updateCurrentlyProcessingJob(Job job) {
        try {
            UUID serverId = backgroundJobServer.getId();
            LOGGER.info("[UPDATE JOBS IN PROGRESS] [serverId:{}] [id:{}] [recurringJobId:{}] [jobName:{}] updating timestamp to keep job alive",
                        serverId, job.getId(), job.getRecurringJobId().orElse(null), job.getJobName());
            return job.updateProcessing();
        } catch (ClassCastException e) {
            // why: there is a tiny chance that the job already succeeded or failed.
            // For example, if the underlying data structure is a concurrent collection and the iteration is weakly
            // consistent, it might return items in its iterator that have already been removed from the collection.
            UUID serverId = backgroundJobServer.getId();
            LOGGER.warn("[UPDATE JOBS IN PROGRESS] [serverId:{}] [id:{}] Job state changed during update (likely completed)",
                       serverId, job.getId());
            return null;
        }
    }
}