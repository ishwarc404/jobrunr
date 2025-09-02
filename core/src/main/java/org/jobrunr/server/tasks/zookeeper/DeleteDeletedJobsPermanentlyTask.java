package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.states.StateName;
import org.jobrunr.server.BackgroundJobServer;

import static java.time.Instant.now;

public class DeleteDeletedJobsPermanentlyTask extends AbstractJobZooKeeperTask {

    public DeleteDeletedJobsPermanentlyTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
    }

    @Override
    protected void runTask() {
        LOGGER.info("[DELETE JOBS PERMANENTLY]: Looking for deleted jobs that can be deleted permanently...");
        int totalAmountOfPermanentlyDeletedJobs = storageProvider.deleteJobsPermanently(StateName.DELETED, now().minus(backgroundJobServerConfiguration().getPermanentlyDeleteDeletedJobsAfter()));
        LOGGER.info("[DELETE JOBS PERMANENTLY]: Found {} deleted jobs that were permanently deleted as part of JobRunr maintenance", totalAmountOfPermanentlyDeletedJobs);
    }
}
