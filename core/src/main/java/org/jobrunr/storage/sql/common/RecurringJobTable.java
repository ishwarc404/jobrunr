package org.jobrunr.storage.sql.common;

import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.sql.common.db.Dialect;
import org.jobrunr.storage.sql.common.db.Sql;
import org.jobrunr.storage.sql.common.db.SqlResultSet;
import java.util.stream.Collectors;
import java.util.logging.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.jobrunr.storage.StorageProviderUtils.RecurringJobs.*;

public class RecurringJobTable extends Sql<RecurringJob> {

    private final JobMapper jobMapper;
    private static final Logger LOGGER = Logger.getLogger(RecurringJobTable.class.getName());

    public RecurringJobTable(Connection connection, Dialect dialect, String tablePrefix, JobMapper jobMapper) {
        this.jobMapper = jobMapper;
        this
                .using(connection, dialect, tablePrefix, "jobrunr_recurring_jobs")
                .with(FIELD_JOB_AS_JSON, jobMapper::serializeRecurringJob)
                .with(FIELD_CREATED_AT, recurringJob -> recurringJob.getCreatedAt().toEpochMilli())
                .with("hourOfExecutionBits", RecurringJob::getHourOfExecutionBitSet);
    }

    public RecurringJobTable withId(String id) {
        with(FIELD_ID, id);
        return this;
    }

    public RecurringJob save(RecurringJob recurringJob) throws SQLException {
        withId(recurringJob.getId());

        if (selectExists("from jobrunr_recurring_jobs where id = :id")) {
            update(recurringJob, "jobrunr_recurring_jobs SET jobAsJson = :jobAsJson, createdAt = :createdAt, hourOfExecutionBits = :hourOfExecutionBits WHERE id = :id");
        } else {
            insert(recurringJob, "into jobrunr_recurring_jobs values(:id, 1, :jobAsJson, :createdAt, :hourOfExecutionBits)");
        }
        return recurringJob;
    }

    public List<RecurringJob> selectAll() {
        return select("jobAsJson from jobrunr_recurring_jobs ORDER BY createdAt ASC")
                .map(this::toRecurringJob)
                .collect(toList());
    }

    public List<RecurringJob> selectByHourMask(long hourMask) {
        with("hourMask", hourMask);
        return select("jobAsJson FROM jobrunr_recurring_jobs WHERE (hourOfExecutionBits & :hourMask) > 0 ORDER BY createdAt ASC")
                .map(this::toRecurringJob)
                .collect(toList());
    }

    public Long selectHashByHourMask(long hourMask) throws SQLException {
        with("hourMask", hourMask);
        return select("SUM(createdAt) as hash FROM jobrunr_recurring_jobs WHERE (hourOfExecutionBits & :hourMask) > 0")
                .mapToLong(resultSet -> resultSet.asLong("hash"))
                .findFirst()
                .orElse(0L);
    }

    public Map<Long, Long> selectHashWindowsByHourMask(long hourMask) throws SQLException {
        with("hourMask", hourMask);
        // This would need more complex implementation based on your windowing logic
        // For now, return empty map as placeholder
        return new java.util.HashMap<>();
    }

    public List<RecurringJob> selectPageByHourMask(long windowStart, long windowEnd, long hourMask) throws SQLException {
        with("hourMask", hourMask);
        with("windowStart", windowStart);
        with("windowEnd", windowEnd);
        return select("jobAsJson FROM jobrunr_recurring_jobs WHERE (hourOfExecutionBits & :hourMask) > 0 AND createdAt >= :windowStart AND createdAt < :windowEnd ORDER BY createdAt ASC")
                .map(this::toRecurringJob)
                .collect(toList());
    }
    
    // Custom function to select recurring jobs by ID
    public List<RecurringJob> selectOne(String id) {
        withId(id);
        return select("jobAsJson FROM jobrunr_recurring_jobs WHERE id = :id ORDER BY createdAt ASC")
                .map(this::toRecurringJob)
                .collect(toList());
    }

    /**
     * Fetch all jobs whose createdAt (epoch seconds)
     * lies in [windowStartEpoch, windowEndEpoch).
     */
    public List<RecurringJob> selectFixedPage(long windowStartEpoch, long windowEndEpoch) {
        String fragment =
        "jobAsJson " +
        "FROM jobrunr_recurring_jobs " +
        "WHERE createdAt >= " + windowStartEpoch + " " +
        "AND createdAt < "   + windowEndEpoch   + " " +
        "ORDER BY createdAt ASC";

        return select(fragment)
    .map(this::toRecurringJob)
        .collect(Collectors.toList());
    }

    public long selectCount() throws SQLException {
        return selectCount("from jobrunr_recurring_jobs");
    }

    public List<RecurringJob> selectAllWithPagination(long offset, int limit) throws SQLException {
        with("limit", limit);
        with("offset", offset);
        return select("jobAsJson from jobrunr_recurring_jobs ORDER BY createdAt ASC " + dialect.limitAndOffset())
                .map(this::toRecurringJob)
                .collect(toList());
    }

    public long count() throws SQLException {
        return selectCount("from jobrunr_recurring_jobs");
    }

    public int deleteById(String id) throws SQLException {
        return withId(id)
                .delete("from jobrunr_recurring_jobs where id = :id");
    }

    private RecurringJob toRecurringJob(SqlResultSet resultSet) {
        return jobMapper.deserializeRecurringJob(resultSet.asString(FIELD_JOB_AS_JSON));
    }


}
