package org.jobrunr.jobs.filters;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.scheduling.exceptions.JobNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.Instant.now;
import static org.jobrunr.jobs.states.StateName.FAILED_STATES;

/**
 * A JobFilter of type {@link ElectStateFilter} that will retry the job if it fails for up to 10 times with an exponential back-off policy.
 * This JobFilter is added by default in JobRunr.
 * <p>
 * If you want to configure the amount of retries, create a new instance and pass it to the JobRunrConfiguration, e.g.:
 *
 * <pre>
 *     JobRunr.configure()
 *                 ...
 *                 .withJobFilter(new RetryFilter(20, 4)) // this will result in 20 retries and the retries will happen after 4 seconds, 16 seconds, 64 seconds, ...
 *                 ...
 *                 .initialize();
 * </pre>
 */
public class RetryFilter implements ElectStateFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryFilter.class);

    /*
     * Retry policy
     */
    public static final int DEFAULT_BACKOFF_POLICY_TIME_SEED = 3;
    public static final int DEFAULT_NBR_OF_RETRIES = 10;

    private final int numberOfRetries;
    private final int backOffPolicyTimeSeed;

    public RetryFilter() {
        this(DEFAULT_NBR_OF_RETRIES);
    }

    public RetryFilter(int numberOfRetries) {
        this(numberOfRetries, DEFAULT_BACKOFF_POLICY_TIME_SEED);
    }

    public RetryFilter(int numberOfRetries, int backOffPolicyTimeSeed) {
        this.numberOfRetries = numberOfRetries;
        this.backOffPolicyTimeSeed = backOffPolicyTimeSeed;
    }

    @Override
    public void onStateElection(Job job, JobState newState) {
        if (isNotFailed(newState) || isJobNotFoundException(newState) || isProblematicExceptionAndMustNotRetry(newState)) {
            LOGGER.info("[JOB RETRY]: Will not retry as either job might not be failed, or job is not found, or problematic exception.");
            return;
        }

        if (maxAmountOfRetriesReached(job)) {
            LOGGER.info("[JOB FAILED PERMANENTLY] [JOB RETRY] [id:{}] [recurringJobId:{}] [jobName:{}] - Job has exhausted all {} retries and will not be retried",
                    job.getId(),
                    job.getRecurringJobId().orElse(null),
                    job.getJobName(),
                    getMaxNumberOfRetries(job));
            return;
        }

        long retryNumber = getFailureCount(job);
        int maxRetries = getMaxNumberOfRetries(job);
        long secondsToAdd = getSecondsToAdd(job);

        LOGGER.info("[JOB RETRY] [id:{}] [recurringJobId:{}] [jobName:{}] - Scheduling retry for job. Retry {} of {} - Will retry in {} seconds",
                job.getId(),
                job.getRecurringJobId().orElse(null),
                job.getJobName(),
                retryNumber,
                maxRetries,
                secondsToAdd);

        job.scheduleAt(now().plusSeconds(secondsToAdd), String.format("Retry %d of %d", retryNumber, maxRetries));
    }

    protected long getSecondsToAdd(Job job) {
        return getExponentialBackoffPolicy(job, backOffPolicyTimeSeed);
    }

    protected long getExponentialBackoffPolicy(Job job, int seed) {
        return (long) Math.pow(seed, getFailureCount(job));
    }

    private boolean maxAmountOfRetriesReached(Job job) {
        return getFailureCount(job) > getMaxNumberOfRetries(job);
    }

    private long getFailureCount(Job job) {
        return job.getJobStates().stream().filter(FAILED_STATES).count();
    }

    private static boolean isJobNotFoundException(JobState newState) {
        if (newState instanceof FailedState) {
            return ((FailedState) newState).getException() instanceof JobNotFoundException;
        }
        return false;
    }

    private static boolean isProblematicExceptionAndMustNotRetry(JobState newState) {
        if (newState instanceof FailedState) {
            return ((FailedState) newState).mustNotRetry();
        }
        return false;
    }

    private static boolean isNotFailed(JobState newState) {
        return !(newState instanceof FailedState);
    }

    private int getMaxNumberOfRetries(Job job) {
        if (job.getAmountOfRetries() != null) return job.getAmountOfRetries();
        return this.numberOfRetries;
    }
}
