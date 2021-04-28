package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.log.v1.CrawlLog;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.util.Strings;

public class CrawlLogAssert extends AbstractAssert<CrawlLogAssert, CrawlLog> {
    public CrawlLogAssert(CrawlLog actual) {
        super(actual, CrawlLogAssert.class);
    }

    public CrawlLogAssert hasWarcId() {
        if (Strings.isNullOrEmpty(actual.getWarcId())) {
            failWithMessage("Expected WarcId to not be empty");
        }
        return this;
    }

    public CrawlLogAssert statusCodeEquals(int expected) {
        if (actual.getStatusCode() != expected) {
            failWithMessage("Expected Status code to be <%d>, but was <%d>",
                    expected, actual.getStatusCode());
        }
        return this;
    }

    public CrawlLogAssert statusCodeEquals(ExtraStatusCodes expected) {
        if (actual.getStatusCode() != expected.getCode()) {
            failWithMessage("Expected Status code to be <%d>, but was <%d>",
                    expected.getCode(), actual.getStatusCode());
        }
        return this;
    }

    public CrawlLogAssert requestedUriEquals(String expected) {
        if (!actual.getRequestedUri().equals(expected)) {
            failWithMessage("Expected Requessted URI to be <%s>, but was <%s>",
                    expected, actual.getRequestedUri());
        }
        return this;
    }

    public ErrorAssert error() {
        return new ErrorAssert(actual.getError());
    }
}
