package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.ProcessingState;
import org.jobrunr.server.BackgroundJobServer;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt;


/*
 * Added this custom function to keep updating the updatedAt time of in progress jobs....
 */
public class UpdateJobsInProgressZooKeeperTask extends AbstractJobZooKeeperTask {

    public UpdateJobsInProgressZooKeeperTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
    }

    @Override
    protected void runTask() {
        LOGGER.debug("[UPDATE INPROGRESS JOB]: Looking for jobs in PROCESSING state to update...");
        
        // Get ALL PROCESSING jobs from storage provider (across all servers)
        List<Job> processingJobs = storageProvider.getJobList(PROCESSING, ascOnUpdatedAt(10000));
        
        if (!processingJobs.isEmpty()) {
            LOGGER.debug("[UPDATE INPROGRESS JOB]: Updating {} currently processed jobs across all servers...", processingJobs.size());
            convertAndProcessJobs(processingJobs, this::updateCurrentlyProcessingJob);
        }
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