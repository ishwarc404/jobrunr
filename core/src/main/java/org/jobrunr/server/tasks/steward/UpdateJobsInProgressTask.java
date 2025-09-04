package org.jobrunr.server.tasks.steward;

import java.util.Set;

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
        String jobIds = jobsInProgress.stream()
                .map(job -> job.getId().toString())
                .collect(java.util.stream.Collectors.joining(", "));
        LOGGER.debug("Updating currently processed jobs. Job IDs: [{}]", jobIds);
        convertAndProcessJobs(jobsInProgress, this::updateCurrentlyProcessingJob);
    }

    private Job updateCurrentlyProcessingJob(Job job) {
        try {
            return job.updateProcessing();
        } catch (ClassCastException e) {
            // why: there is a tiny chance that the job already succeeded or failed.
            // For example, if the underlying data structure is a concurrent collection and the iteration is weakly
            // consistent, it might return items in its iterator that have already been removed from the collection.
            return null;
        }
    }
}