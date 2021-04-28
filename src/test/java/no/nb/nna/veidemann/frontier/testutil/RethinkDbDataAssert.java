package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.MapAssert;

import java.util.List;

public class RethinkDbDataAssert extends AbstractAssert<RethinkDbDataAssert, RethinkDbData> {
    public RethinkDbDataAssert(RethinkDbData actual) {
        super(actual, RethinkDbDataAssert.class);
    }

    public static RethinkDbDataAssert assertThat(RethinkDbData actual) {
        return new RethinkDbDataAssert(actual);
    }

    public RethinkDbDataAssert hasQueueTotalCount(int expected) throws DbQueryException, DbConnectionException {
        List<QueuedUri> val = actual.getQueuedUris();
        if (val.size() != expected) {
            failWithMessage("Expected total queue count to be <%d>, but was <%d>", expected, val.size());
        }
        return this;
    }

    public MapAssert<String, CrawlExecutionStatus> crawlExecutionStatuses() throws DbException {
        return new MapAssert<>(actual.getCrawlExecutionStatuses());
    }

    public MapAssert<String, JobExecutionStatus> jobExecutionStatuses() throws DbException {
        return new MapAssert<>(actual.getJobExecutionStatuses());
    }
}
