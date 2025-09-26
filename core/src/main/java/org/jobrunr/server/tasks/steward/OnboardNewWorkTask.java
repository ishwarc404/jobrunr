package org.jobrunr.server.tasks.steward;

import org.jobrunr.jobs.Job;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.server.strategy.WorkDistributionStrategy;
import org.jobrunr.server.tasks.Task;
import org.jobrunr.server.tasks.TaskRunInfo;
import org.jobrunr.storage.navigation.AmountRequest;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class OnboardNewWorkTask extends AbstractJobStewardTask {

    private final ReentrantLock reentrantLock;
    private final WorkDistributionStrategy workDistributionStrategy;

    public OnboardNewWorkTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.reentrantLock = new ReentrantLock();
        this.workDistributionStrategy = backgroundJobServer.getWorkDistributionStrategy();
    }

    /**
     * As the {@link Task#run(TaskRunInfo)} is not thread-safe due to mutable field <code>runInfo</code> containing
     * the {@link TaskRunInfo}, we use it thread-safe here by not assigning or using the <code>runInfo</code> field.
     */
    public void runTaskThreadSafe() {
        runTask();
    }

    @Override
    protected void runTask() {
        if (backgroundJobServer.isRunning() && reentrantLock.tryLock()) {
            try {
                final AmountRequest workPageRequest = workDistributionStrategy.getWorkPageRequest();
                if (workPageRequest.getLimit() > 0) {
                    // Get worker capacity info
                    int occupiedWorkers = backgroundJobServer.getJobSteward().getOccupiedWorkerCount();
                    int totalWorkers = backgroundJobServer.getConfiguration().getWorkerPoolSize();
                    int availableWorkers = totalWorkers - occupiedWorkers;

                    final List<Job> enqueuedJobs = storageProvider.getJobsToProcess(backgroundJobServer, workPageRequest);

                    if (!enqueuedJobs.isEmpty()) {
                        LOGGER.info("[ONBOARD JOBS] [DISPATCH QUEUE]: Fetched {} jobs | Workers: {}/{} busy | {} available | Requested: {}",
                                   enqueuedJobs.size(),
                                   occupiedWorkers, totalWorkers,
                                   availableWorkers,
                                   workPageRequest.getLimit());

                        if (availableWorkers == 0) {
                            LOGGER.warn("[ONBOARD JOBS] [DISPATCH QUEUE]: WORKER OVERLOAD - All {} workers are busy, {} jobs waiting",
                                       totalWorkers, enqueuedJobs.size());
                        }
                    }

                    enqueuedJobs.forEach(backgroundJobServer::processJob);
                }
            } finally {
                reentrantLock.unlock();
            }
        }
    }
}
