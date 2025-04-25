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
        LOGGER.debug("Looking for scheduled jobs... ");
        System.out.println("Looking for scheduled jobs: ");
        Instant scheduledBefore = now().plus(backgroundJobServerConfiguration().getPollInterval());
        processManyJobs(
            previousResults -> {
                List<Job> jobs = getJobsToSchedule(scheduledBefore, previousResults);
                System.out.println("Fetched " + jobs.size() + " scheduled jobs.");
                return jobs;
            },
            Job::enqueue,
            totalAmountOfEnqueuedJobs -> LOGGER.debug("Found {} scheduled jobs to enqueue.", totalAmountOfEnqueuedJobs));
    }

    private List<Job> getJobsToSchedule(Instant scheduledBefore, List<Job> previousResults) {
        if (previousResults != null && previousResults.size() < pageRequestSize) return emptyList();
        return storageProvider.getScheduledJobs(scheduledBefore, ascOnUpdatedAt(pageRequestSize));
    }
}



// // 
// update jobrunr_jobs SET version = 2, jobAsJson = '{"version":2,"jobSignature":"com.example.GetWeather.fetchSanFranciscoWeather()","jobName":"com.example.GetWeather.fetchSanFranciscoWeather()","labels":[],"jobDetails":{"className":"com.example.GetWeather","methodName":"fetchSanFranciscoWeather","jobParameters":[],"cacheable":true},"id":"01966a22-f70b-7f4c-8468-da97ef2fc79c","jobHistory":[{"@class":"org.jobrunr.jobs.states.ScheduledState","state":"SCHEDULED","createdAt":"2025-04-24T23:29:58.539700Z","scheduledAt":"2025-04-24T23:30:00Z","recurringJobId":"sf-weather-51d1c67f-fea9-4a95-b283-18b9765fa787","reason":"Scheduled by recurring job ''com.example.GetWeather.fetchSanFranciscoWeather()''"},{"@class":"org.jobrunr.jobs.states.EnqueuedState","state":"ENQUEUED","createdAt":"2025-04-24T23:29:58.597801Z"}],"metadata":{"@class":"java.util.concurrent.ConcurrentHashMap"},
// "recurringJobId":"sf-weather-51d1c67f-fea9-4a95-b283-18b9765fa787"}', state = 'ENQUEUED', updatedAt ='2025-04-24 23:29:58.597801', 
// scheduledAt = null WHERE id = '01966a22-f70b-7f4c-8468-da97ef2fc79c' and version = 1