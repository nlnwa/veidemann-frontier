package no.nb.nna.veidemann.frontier.testutil;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import org.assertj.core.api.AbstractAssert;

public class CrawlHostGroupAssert<SELF extends CrawlHostGroupAssert<SELF>> extends AbstractAssert<SELF, CrawlHostGroup> {
    protected final SELF myself;

    public CrawlHostGroupAssert(CrawlHostGroup actual, Class<?> selfType) {
        super(actual, CrawlHostGroupAssert.class);
        myself = (SELF) selfType.cast(this);
    }

    public CrawlHostGroupAssert(CrawlHostGroup actual) {
        this(actual, CrawlHostGroupAssert.class);
    }

    public static CrawlHostGroupAssert assertThat(CrawlHostGroup actual) {
        return new CrawlHostGroupAssert(actual);
    }

    public SELF isEqualTo(CrawlHostGroup expected) {
        if (!actual.equals(expected)) {
            failWithMessage("Expected CrawlHostGroup <%s> to be equal to <%s>, but was not",
                    actual.getId(), actual, expected);
        }
        return myself;
    }

    public SELF hasQueueCount(int expected) {
        long val = actual.getQueuedUriCount();
        if (val != expected) {
            failWithMessage("Expected queue count for crawl host group <%s> to be <%d>, but was <%d>",
                    actual.getId(), expected, val);
        }
        return myself;
    }

    public SELF hasSessionToken(String expected) {
        String val = actual.getSessionToken();
        if (!val.equals(expected)) {
            failWithMessage("Expected session token for crawl host group <%s> to be <%s>, but was <%s>",
                    actual.getId(), expected, val);
        }
        return myself;
    }

    public SELF hasCurrentUriId(String expected) {
        String val = actual.getCurrentUriId();
        if (!val.equals(expected)) {
            failWithMessage("Expected current uri id for crawl host group <%s> to be <%s>, but was <%s>",
                    actual.getId(), expected, val);
        }
        return myself;
    }

    public SELF hasFetchStartTimeStamp(Timestamp expected) {
        Timestamp val = actual.getFetchStartTimeStamp();
        if (!val.equals(expected)) {
            failWithMessage("Expected Fetch Start Timestamp for crawl host group <%s> to be <%s>, but was <%s>",
                    actual.getId(), Timestamps.toString(expected), Timestamps.toString(val));
        }
        return myself;
    }

    public SELF hasPolitenessValues(long minTimeBetweenPageLoadMs, long maxTimeBetweenPageLoadMs, int maxRetries, int retryDelaySeconds, float delayFactor) {
        if (actual.getMinTimeBetweenPageLoadMs() != minTimeBetweenPageLoadMs) {
            failWithMessage("Expected MinTimeBetweenPageLoadMs for crawl host group <%s> to be <%d>, but was <%d>",
                    actual.getId(), minTimeBetweenPageLoadMs, actual.getMinTimeBetweenPageLoadMs());
        }
        if (actual.getMaxTimeBetweenPageLoadMs() != maxTimeBetweenPageLoadMs) {
            failWithMessage("Expected MaxTimeBetweenPageLoadMs for crawl host group <%s> to be <%d>, but was <%d>",
                    actual.getId(), maxTimeBetweenPageLoadMs, actual.getMaxTimeBetweenPageLoadMs());
        }
        if (actual.getMaxRetries() != maxRetries) {
            failWithMessage("Expected MaxRetries for crawl host group <%s> to be <%d>, but was <%d>",
                    actual.getId(), maxRetries, actual.getMaxRetries());
        }
        if (actual.getRetryDelaySeconds() != retryDelaySeconds) {
            failWithMessage("Expected RetryDelaySeconds for crawl host group <%s> to be <%d>, but was <%d>",
                    actual.getId(), retryDelaySeconds, actual.getRetryDelaySeconds());
        }
        if (actual.getDelayFactor() != delayFactor) {
            failWithMessage("Expected DelayFactor for crawl host group <%s> to be <%s>, but was <%s>",
                    actual.getId(), delayFactor, actual.getDelayFactor());
        }
        return myself;
    }
}
