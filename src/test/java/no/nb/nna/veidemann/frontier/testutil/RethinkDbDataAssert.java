package no.nb.nna.veidemann.frontier.testutil;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.data.TemporalOffset;
import redis.clients.jedis.Tuple;

import java.time.Instant;
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

    public CrawlLogsAssert crawlLogs() throws DbQueryException, DbConnectionException {
        return new CrawlLogsAssert(actual.getCrawlLogs());
    }

    public static class CrawlLogsAssert extends AbstractAssert<CrawlLogsAssert, List<CrawlLog>> {
        public CrawlLogsAssert(List<CrawlLog> actual) {
            super(actual, CrawlLogsAssert.class);
        }

        public CrawlLogsAssert hasNumberOfElements(int expected) {
            if (actual.size() != expected) {
                failWithMessage("Expected number of CrawlLogs to be <%d>, but was <%d>",
                        expected, actual.size());
            }
            return this;
        }

//        public CrawlHostGroupFromMapAssert id(String id) {
//            return new CrawlHostGroupFromMapAssert(this, actual.get(id));
//        }
    }

//    public static class CrawlHostGroupFromMapAssert extends CrawlHostGroupAssert<CrawlHostGroupFromMapAssert> {
//        final CrawlHostGroupMapAssert origin;
//
//        public CrawlHostGroupFromMapAssert(CrawlHostGroupMapAssert origin, CrawlHostGroup actual) {
//            super(actual);
//            this.origin = origin;
//        }
//
//        public CrawlHostGroupFromMapAssert id(String id) {
//            return origin.id(id);
//        }
//    }

    public static class DelayQueueAssert extends AbstractAssert<DelayQueueAssert, List<Tuple>> {
        final String name;

        public DelayQueueAssert(String name, List<Tuple> actual) {
            super(actual, DelayQueueAssert.class);
            this.name = name;
        }

        public DelayQueueAssert hasNumberOfElements(int expected) {
            if (actual.size() != expected) {
                failWithMessage("Expected number of elements for %s to be <%d>, but was <%d>",
                        name, expected, actual.size());
            }
            return this;
        }

        public DelayQueueElementAssert element(int elementNumber) {
            return new DelayQueueElementAssert(this, elementNumber, actual.get(elementNumber));
        }
    }

    public static class DelayQueueElementAssert extends AbstractAssert<DelayQueueElementAssert, Tuple> {
        final DelayQueueAssert origin;
        final int elementNumber;

        public DelayQueueElementAssert(DelayQueueAssert origin, int elementNumber, Tuple actual) {
            super(actual, DelayQueueElementAssert.class);
            this.origin = origin;
            this.elementNumber = elementNumber;
        }

        public DelayQueueElementAssert element(int elementNumber) {
            return origin.element(elementNumber);
        }

//        public DelayQueueElementAssert hasScore(double expected) {
//            if (actual.getScore() != expected) {
//                failWithMessage("Expected score for element #%d of %s to be <%f>, but was <%f>",
//                        elementNumber, origin.name, expected, actual.getScore());
//            }
//            return this;
//        }

        public DelayQueueElementAssert hasTimestamp(Timestamp expected) {
            double expectedScore = Long.valueOf(Timestamps.toMillis(expected)).doubleValue();
            if (actual.getScore() != expectedScore) {
                Instant expectedTime = Instant.ofEpochMilli(Double.valueOf(expectedScore).longValue());
                Instant actualTime = Instant.ofEpochMilli(Double.valueOf(actual.getScore()).longValue());
                failWithMessage("Expected timestamp for element #%d of %s to be <%s>, but was <%s>",
                        elementNumber, origin.name, expectedTime, actualTime);
            }
            return this;
        }

        public DelayQueueElementAssert hasTimestampCloseTo(Timestamp expected, TemporalOffset offset) {
            double expectedScore = Long.valueOf(Timestamps.toMillis(expected)).doubleValue();
            Instant expectedTime = Instant.ofEpochMilli(Double.valueOf(expectedScore).longValue());
            Instant actualTime = Instant.ofEpochMilli(Double.valueOf(actual.getScore()).longValue());
            if (offset.isBeyondOffset(actualTime, expectedTime)) {
                failWithMessage("Expected timestamp for element #%d of %s to be close to <%s> %s. Actual timestamp was <%s>",
                        elementNumber, origin.name, expectedTime, offset.getBeyondOffsetDifferenceDescription(expectedTime, actualTime), actualTime);
            }
            return this;
        }

        public DelayQueueElementAssert hasValue(String expected) {
            if (!actual.getElement().equals(expected)) {
                failWithMessage("Expected value for element #%d of %s to be <%s>, but was <%s>",
                        elementNumber, origin.name, expected, actual.getElement());
            }
            return this;
        }
    }
}
