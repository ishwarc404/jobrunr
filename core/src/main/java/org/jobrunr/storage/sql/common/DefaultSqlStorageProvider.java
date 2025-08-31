package org.jobrunr.storage.sql.common;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.filters.JobFilterUtils;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.AbstractStorageProvider;
import org.jobrunr.storage.BackgroundJobServerStatus;
import org.jobrunr.storage.ConcurrentJobModificationException;
import org.jobrunr.storage.JobNotFoundException;
import org.jobrunr.storage.JobRunrMetadata;
import org.jobrunr.storage.JobStats;
import org.jobrunr.storage.RecurringJobsResult;
import org.jobrunr.storage.StorageException;
import org.jobrunr.storage.StorageProviderUtils.DatabaseOptions;
import org.jobrunr.storage.StorageProviderUtils.RecurringJobs;
import org.jobrunr.storage.navigation.AmountRequest;
import org.jobrunr.storage.sql.SqlStorageProvider;
import org.jobrunr.storage.sql.common.db.Dialect;
import org.jobrunr.storage.sql.common.db.Transaction;
import org.jobrunr.utils.resilience.RateLimiter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.storage.StorageProviderUtils.DatabaseOptions.CREATE;
import static org.jobrunr.storage.StorageProviderUtils.DatabaseOptions.SKIP_CREATE;
import static org.jobrunr.utils.resilience.RateLimiter.Builder.rateLimit;
import static org.jobrunr.utils.resilience.RateLimiter.SECOND;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSqlStorageProvider extends AbstractStorageProvider implements SqlStorageProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlStorageProvider.class);

    protected final DataSource dataSource;
    protected final Dialect dialect;
    protected final String tablePrefix;
    private JobMapper jobMapper;

    public DefaultSqlStorageProvider(DataSource dataSource, Dialect dialect, DatabaseOptions databaseOptions) {
        this(dataSource, dialect, databaseOptions, rateLimit().at1Request().per(SECOND));
    }

    public DefaultSqlStorageProvider(DataSource dataSource, Dialect dialect, String tablePrefix, DatabaseOptions databaseOptions) {
        this(dataSource, dialect, tablePrefix, databaseOptions, rateLimit().at1Request().per(SECOND));
    }

    public DefaultSqlStorageProvider(DataSource dataSource, Dialect dialect, DatabaseOptions databaseOptions, RateLimiter changeListenerNotificationRateLimit) {
        this(dataSource, dialect, null, databaseOptions, changeListenerNotificationRateLimit);
    }

    DefaultSqlStorageProvider(DataSource dataSource, Dialect dialect, String tablePrefix, DatabaseOptions databaseOptions, RateLimiter changeListenerNotificationRateLimit) {
        super(changeListenerNotificationRateLimit);
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.tablePrefix = tablePrefix;
        setUpStorageProvider(databaseOptions);
    }

    @Override
    public void setJobMapper(JobMapper jobMapper) {
        this.jobMapper = jobMapper;
    }

    @Override
    public void setUpStorageProvider(DatabaseOptions databaseOptions) {
        if (databaseOptions == CREATE) {
            getDatabaseCreator()
                    .runMigrations();
        } else if (databaseOptions == SKIP_CREATE) {
            getDatabaseCreator()
                    .validateTables();
        }
    }

    @Override
    public void announceBackgroundJobServer(BackgroundJobServerStatus serverStatus) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            backgroundJobServerTable(conn).announce(serverStatus);
            transaction.commit();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public boolean signalBackgroundJobServerAlive(BackgroundJobServerStatus serverStatus) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final boolean isServerAlive = backgroundJobServerTable(conn).signalServerAlive(serverStatus);
            transaction.commit();
            return isServerAlive;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void signalBackgroundJobServerStopped(BackgroundJobServerStatus serverStatus) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            backgroundJobServerTable(conn).signalServerStopped(serverStatus);
            transaction.commit();
        } catch (SQLException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public List<BackgroundJobServerStatus> getBackgroundJobServers() {
        try (final Connection conn = dataSource.getConnection()) {
            return backgroundJobServerTable(conn).getAll();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public UUID getLongestRunningBackgroundJobServerId() {
        try (final Connection conn = dataSource.getConnection()) {
            return backgroundJobServerTable(conn).getLongestRunningBackgroundJobServerId();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public int removeTimedOutBackgroundJobServers(Instant heartbeatOlderThan) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final int deletedBackgroundJobServers = backgroundJobServerTable(conn).removeAllWithLastHeartbeatOlderThan(heartbeatOlderThan);
            transaction.commit();
            return deletedBackgroundJobServers;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void saveMetadata(JobRunrMetadata metadata) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            metadataTable(conn).save(metadata);
            transaction.commit();
            notifyMetadataChangeListeners();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<JobRunrMetadata> getMetadata(String name) {
        try (final Connection conn = dataSource.getConnection()) {
            return metadataTable(conn).getAll(name);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public JobRunrMetadata getMetadata(String name, String owner) {
        try (final Connection conn = dataSource.getConnection()) {
            return metadataTable(conn).get(name, owner);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteMetadata(String name) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final int amountDeleted = metadataTable(conn).deleteByName(name);
            transaction.commit();
            notifyMetadataChangeListeners(amountDeleted > 0);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Job save(Job jobToSave) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final Job savedJob = jobTable(conn).save(jobToSave);
            transaction.commit();
            notifyJobStatsOnChangeListeners();
            return savedJob;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }


    //Rewrite batched statements helps optimise this.
    @Override
    public List<Job> save(List<Job> jobs) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            try {
                long start = System.currentTimeMillis();
                final List<Job> savedJobs = jobTable(conn).save(jobs);
                long duration = System.currentTimeMillis() - start;
                LOGGER.info("Inserted: " + jobs.size() + " jobs in: " + duration + "ms");
                transaction.commit();
                notifyJobStatsOnChangeListenersIf(!jobs.isEmpty());
                return savedJobs;
            } catch (ConcurrentJobModificationException e) {
                // even in case of a ConcurrentJobModificationException, we still want to commit the jobs that were saved successfully
                // to be compatible with NoSQL databases
                transaction.commit();
                throw e;
            }
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    //Function to read job from jobrunr_jobs table by its ID
    @Override
    public Job getJobById(UUID id) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn)
                    .selectJobById(id)
                    .orElseThrow(() -> new JobNotFoundException(id));
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    //Custom function to get the scheduledAt time of a job by its ID
    @Override
    public Instant getJobScheduledAt(UUID jobId) {
        String sql = "SELECT scheduledAt FROM jobrunr_jobs WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, jobId.toString());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("scheduledAt");
                    return ts != null ? ts.toInstant() : null;
                }
                return null;
            }

        } catch (SQLException e) {
            LOGGER.error("Error querying scheduledAt for job: {}", jobId, e);
            throw new StorageException(e);
        }
    }


    @Override
    public long countJobs(StateName state) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn).countJobs(state);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<Job> getJobList(StateName state, Instant updatedBefore, AmountRequest amountRequest) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn).selectJobsByState(state, updatedBefore, amountRequest);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<Job> getJobList(StateName state, AmountRequest amountRequest) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn).selectJobsByState(state, amountRequest);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<Job> getScheduledJobs(Instant scheduledBefore, AmountRequest amountRequest) {
        try (final Connection conn = dataSource.getConnection()) {
            long start = System.currentTimeMillis();
            LOGGER.info("[SCHEDULED JOBS]: Fetching jobs to schedule..");
            final List<Job> savedJobs = jobTable(conn).selectJobsScheduledBefore(scheduledBefore, amountRequest);
            long duration = System.currentTimeMillis() - start;
            LOGGER.info("[SCHEDULED JOBS]: Fetched " + savedJobs.size() + " jobs in: " + duration + "ms");
            return savedJobs;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<Job> getJobsToProcess(BackgroundJobServer backgroundJobServer, AmountRequest amountRequest) {
        JobFilterUtils jobFilterUtils = new JobFilterUtils(backgroundJobServer.getJobFilters());
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            List<Job> jobs = jobTable(conn).selectJobsToProcess(amountRequest);
            try {
                jobs.forEach(job -> job.startProcessingOn(backgroundJobServer));
                jobFilterUtils.runOnStateElectionFilter(jobs);
                List<Job> jobsToProcess = jobTable(conn).save(jobs);
                transaction.commit();
                jobFilterUtils.runOnStateAppliedFilters(jobsToProcess);
                return jobsToProcess.stream().filter(job -> job.hasState(PROCESSING)).collect(toList());
            } catch (ConcurrentJobModificationException e) {
                List<Job> actualSavedJobs = new ArrayList<>(jobs);
                Set<UUID> concurrentUpdatedJobIds = e.getConcurrentUpdatedJobs().stream().map(Job::getId).collect(toSet());
                actualSavedJobs.removeIf(j -> concurrentUpdatedJobIds.contains(j.getId()));
                transaction.commit();
                jobFilterUtils.runOnStateAppliedFilters(actualSavedJobs);
                return actualSavedJobs.stream().filter(job -> job.hasState(PROCESSING)).collect(toList());
            }
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public int deletePermanently(UUID id) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final int amountDeleted = jobTable(conn).deletePermanently(id);
            transaction.commit();
            notifyJobStatsOnChangeListenersIf(amountDeleted > 0);
            return amountDeleted;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public int deleteJobsPermanently(StateName state, Instant updatedBefore) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final int amountDeleted = jobTable(conn).deleteJobsByStateAndUpdatedBefore(state, updatedBefore);
            transaction.commit();
            notifyJobStatsOnChangeListenersIf(amountDeleted > 0);
            return amountDeleted;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Set<String> getDistinctJobSignatures(StateName... states) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn).getDistinctJobSignatures(states);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public boolean recurringJobExists(String recurringJobId, StateName... states) {
        try (final Connection conn = dataSource.getConnection()) {
            return jobTable(conn).recurringJobExists(recurringJobId, states);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    public Map<String, Long> recurringJobsExists(StateName... states) {
    
        String sql =
            "SELECT recurringJobId, COUNT(*) AS jobCount " +
            "  FROM jobrunr_jobs " +
            " WHERE state IN ('SCHEDULED','ENQUEUED','PROCESSING') " +
            " GROUP BY recurringJobId";
    
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
    
            Map<String, Long> counts = new HashMap<>();
            while (rs.next()) {
                String id = rs.getString("recurringJobId");
                long cnt = rs.getLong("jobCount");
                counts.put(id, cnt);
            }
            return counts;
    
        } catch (SQLException e) {
            LOGGER.error("Error running recurringJobsExists");
            e.printStackTrace();
            throw new StorageException(e);
        }
    }

    @Override
    public Map<String, Long> recurringJobsExistsByHours(long hourMask, StateName... states) {

        long start = System.currentTimeMillis();
        LOGGER.info("[RECURRING JOBS]: Fetching existance report..");

        String sql =
            "SELECT j.recurringJobId, COUNT(*) AS jobCount " +
            "  FROM jobrunr_jobs j " +
            " WHERE j.state IN ('SCHEDULED','ENQUEUED','PROCESSING') " +
            "   AND j.recurringJobId IN (" +
            "     SELECT id FROM jobrunr_recurring_jobs " +
            "     WHERE (hourOfExecutionBits & ?) > 0" +
            "   ) " +
            " GROUP BY j.recurringJobId";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setLong(1, hourMask);
            
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, Long> counts = new HashMap<>();
                while (rs.next()) {
                    String id = rs.getString("recurringJobId");
                    long cnt = rs.getLong("jobCount");
                    counts.put(id, cnt);
                }

                long duration = System.currentTimeMillis() - start;
                LOGGER.info("[RECURRING JOBS]: Fetched existance report in: " + duration + "ms");
                return counts;
            }

        } catch (SQLException e) {
            LOGGER.error("Error running recurringJobsExistsByHours", e);
            throw new StorageException(e);
        }
    }
    
    public Instant getLastSucceedJobUpdateTime() {
        String sql = "SELECT updatedAt FROM jobrunr_jobs WHERE state = 'SUCCEEDED' ORDER BY updatedAt DESC LIMIT 1";
    
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
    
            if (rs.next()) {
                // Use UTC timezone to get the correct timestamp as it is stored in UTC in the db
                // and convert it to Instant which is again UTC technically
                Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                Timestamp timestamp = rs.getTimestamp("updatedAt", utcCalendar);
                return timestamp.toInstant();
            } else {
                // No succeeded jobs found, return epoch or throw exception depending on your logic
                LOGGER.warn("No SUCCEEDED jobs found in jobrunr_jobs.");
                return Instant.EPOCH;
            }
    
        } catch (SQLException e) {
            LOGGER.error("Error in getLastSucceedJobUpdateTime", e);
            throw new StorageException(e);
        }
    }
    

    @Override
    public RecurringJob saveRecurringJob(RecurringJob recurringJob) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final RecurringJob savedRecurringJob = recurringJobTable(conn).save(recurringJob);
            transaction.commit();
            return savedRecurringJob;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public RecurringJobsResult getRecurringJobs() {
        try (final Connection conn = dataSource.getConnection()) {
            RecurringJobsResult result = new RecurringJobsResult(recurringJobTable(conn).selectAll());
            return result;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public RecurringJobsResult getRecurringJobsByHours(long hourMask) {
        try (final Connection conn = dataSource.getConnection()) {
            RecurringJobsResult result = new RecurringJobsResult(recurringJobTable(conn).selectByHourMask(hourMask));
            return result;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public boolean recurringJobsUpdatedByHours(Long recurringJobsUpdatedHash, long hourMask) {
        try (final Connection conn = dataSource.getConnection()) {
            Long currentHash = recurringJobTable(conn).selectHashByHourMask(hourMask);
            return !recurringJobsUpdatedHash.equals(currentHash);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Map<Long, Long> getRecurringJobsHashByHours(long hourMask) {
        String sql =
        "WITH RECURSIVE\n" +
        "  first_ts AS (\n" +
        "    SELECT MIN(createdAt) AS ts, MAX(createdAt) AS max_ts FROM jobrunr_recurring_jobs WHERE (hourOfExecutionBits & ?) > 0\n" +
        "  ),\n" +
        "  bucket_defs AS (\n" +
        "    SELECT ts, CEIL((max_ts - ts) / 43200000) AS total_buckets FROM first_ts\n" +
        "  ),\n" +
        "  seq AS (\n" +
        "    SELECT 0 AS bucket_idx FROM bucket_defs\n" +
        "    UNION ALL\n" +
        "    SELECT bucket_idx + 1 FROM seq JOIN bucket_defs ON bucket_idx + 1 < bucket_defs.total_buckets\n" +
        "  ),\n" +
        "  aggregates AS (\n" +
        "    SELECT (j.createdAt - f.ts) DIV 43200000 AS bucket_idx, SUM(j.createdAt) AS window_hash\n" +
        "    FROM jobrunr_recurring_jobs j CROSS JOIN first_ts f\n" +
        "    WHERE (j.hourOfExecutionBits & ?) > 0\n" +
        "    GROUP BY bucket_idx\n" +
        "  )\n" +
        "SELECT (f.ts + s.bucket_idx * 43200000) AS window_start_epoch, COALESCE(a.window_hash, 0) AS window_hash\n" +
        "FROM seq s CROSS JOIN first_ts f LEFT JOIN aggregates a USING(bucket_idx)\n" +
        "ORDER BY s.bucket_idx";
    
        Map<Long, Long> windowHashMap = new LinkedHashMap<>();
    
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setLong(1, hourMask);  // First hourMask parameter
            ps.setLong(2, hourMask);  // Second hourMask parameter
            
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long windowStartEpoch = rs.getLong("window_start_epoch");
                    long windowHash = rs.getLong("window_hash");
                    windowHashMap.put(windowStartEpoch, windowHash);
                }
            }
    
        } catch (SQLException e) {
            LOGGER.error("Error running getRecurringJobsHashByHours", e);
            throw new StorageException(e);
        }
    
        return windowHashMap;
    }

    @Override
    public List<RecurringJob> getRecurringJobsPageByHours(long windowStart, long windowEnd, long hourMask) {
        try (final Connection conn = dataSource.getConnection()) {
            return recurringJobTable(conn).selectPageByHourMask(windowStart, windowEnd, hourMask);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long countRecurringJobs() {
        try (final Connection conn = dataSource.getConnection()) {
            return recurringJobTable(conn).selectCount();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public List<RecurringJob> getRecurringJobsBatch(long offset, int limit) {
        try (final Connection conn = dataSource.getConnection()) {
            return recurringJobTable(conn).selectAllWithPagination(offset, limit);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    // Custom Function to get the details of a recurring job by its ID
    @Override
    public RecurringJobsResult getRecurringJobById(String id) {
       try (final Connection conn = dataSource.getConnection()) {
            RecurringJobsResult result = new RecurringJobsResult(recurringJobTable(conn).selectOne(id));
            return result;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    public RecurringJobsResult getRecurringJobsPage(long windowStartEpoch, long windowEndEpoch) {
        try (final Connection conn = dataSource.getConnection()) {
            RecurringJobsResult result = new RecurringJobsResult(recurringJobTable(conn).selectFixedPage(windowStartEpoch,windowEndEpoch));
            return result;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    public Map<Long, Long> getRecurringJobsHash() {
        String sql =
        "WITH RECURSIVE\n" +
        "  first_ts AS (\n" +
        "    SELECT MIN(createdAt) AS ts, MAX(createdAt) AS max_ts FROM jobrunr_recurring_jobs\n" +
        "  ),\n" +
        "  bucket_defs AS (\n" +
        "    SELECT ts, CEIL((max_ts - ts) / 43200000) AS total_buckets FROM first_ts\n" +
        "  ),\n" +
        "  seq AS (\n" +
        "    SELECT 0 AS bucket_idx FROM bucket_defs\n" +
        "    UNION ALL\n" +
        "    SELECT bucket_idx + 1 FROM seq JOIN bucket_defs ON bucket_idx + 1 < bucket_defs.total_buckets\n" +
        "  ),\n" +
        "  aggregates AS (\n" +
        "    SELECT (j.createdAt - f.ts) DIV 43200000 AS bucket_idx, SUM(j.createdAt) AS window_hash\n" +
        "    FROM jobrunr_recurring_jobs j CROSS JOIN first_ts f\n" +
        "    GROUP BY bucket_idx\n" +
        "  )\n" +
        "SELECT (f.ts + s.bucket_idx * 43200000) AS window_start_epoch, COALESCE(a.window_hash, 0) AS window_hash\n" +
        "FROM seq s CROSS JOIN first_ts f LEFT JOIN aggregates a USING(bucket_idx)\n" +
        "ORDER BY s.bucket_idx";
    
        Map<Long, Long> windowHashMap = new LinkedHashMap<>();
    
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
    
            while (rs.next()) {
                long windowStartEpoch = rs.getLong("window_start_epoch");
                long windowHash = rs.getLong("window_hash");
                windowHashMap.put(windowStartEpoch, windowHash);
            }
    
        } catch (SQLException e) {
            LOGGER.error("Error running getRecurringJobsHash", e);
            throw new StorageException(e);
        }
    
        return windowHashMap;
    }
    


    @Override
    public boolean recurringJobsUpdated(Long recurringJobsUpdatedHash) {
        try (final Connection conn = dataSource.getConnection()) {
            Long lastModifiedHash = recurringJobTable(conn).selectSum(RecurringJobs.FIELD_CREATED_AT);
            return !recurringJobsUpdatedHash.equals(lastModifiedHash);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    public Long recurringJobsUpdatedHash(long windowStartEpoch, long windowEndEpoch) {
        try (final Connection conn = dataSource.getConnection()) {
            Long lastModifiedHash = recurringJobTable(conn).selectSumWithLimitOffset(RecurringJobs.FIELD_CREATED_AT, windowStartEpoch, windowEndEpoch);
            return lastModifiedHash;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public int deleteRecurringJob(String id) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            final int deletedRecurringJobCount = recurringJobTable(conn).deleteById(id);
            transaction.commit();
            return deletedRecurringJobCount;
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public JobStats getJobStats() {
        try (final Connection conn = dataSource.getConnection()) {
            return jobStatsView(conn).getJobStats();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void publishTotalAmountOfSucceededJobs(int amount) {
        try (final Connection conn = dataSource.getConnection(); final Transaction transaction = new Transaction(conn)) {
            metadataTable(conn).incrementCounter("succeeded-jobs-counter-cluster", amount);
            transaction.commit();
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    protected DatabaseCreator getDatabaseCreator() {
        return new DatabaseCreator(dataSource, tablePrefix, getClass());
    }

    protected JobTable jobTable(Connection connection) {
        return new JobTable(connection, dialect, tablePrefix, jobMapper);
    }

    protected RecurringJobTable recurringJobTable(Connection connection) {
        return new RecurringJobTable(connection, dialect, tablePrefix, jobMapper);
    }

    protected BackgroundJobServerTable backgroundJobServerTable(Connection connection) {
        return new BackgroundJobServerTable(connection, dialect, tablePrefix);
    }

    protected MetadataTable metadataTable(Connection connection) {
        return new MetadataTable(connection, dialect, tablePrefix);
    }

    protected JobStatsView jobStatsView(Connection connection) {
        return new JobStatsView(connection, dialect, tablePrefix);
    }
}
