package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.LongAssert;

import java.util.Objects;
import java.util.function.Consumer;

public class CrawlExecutionStatusAssert extends AbstractAssert<CrawlExecutionStatusAssert, CrawlExecutionStatus> {

    public CrawlExecutionStatusAssert(CrawlExecutionStatus actual) {
        super(actual, CrawlExecutionStatusAssert.class);
    }

    public CrawlExecutionStatusAssert hasState(State expected) {
        isNotNull();
        if (!Objects.equals(actual.getState(), expected)) {
            failWithMessage("Expected state to be <%s> but was <%s>", expected, actual.getState());
        }

        return this;
    }

    public CrawlExecutionStatusAssert hasCreatedTime(boolean expected) {
        isNotNull();
        if (actual.hasCreatedTime() != expected) {
            failWithMessage("Expected hasCreatedTime to be <%b> but was <%b>", expected, actual.getCreatedTime());
        }

        return this;
    }

    public CrawlExecutionStatusAssert hasStartTime(boolean expected) {
        isNotNull();
        if (actual.hasStartTime() != expected) {
            failWithMessage("Expected hasStartTime to be <%b> but was <%b>", expected, actual.hasStartTime());
        }

        return this;
    }

    public CrawlExecutionStatusAssert hasEndTime(boolean expected) {
        isNotNull();
        if (actual.hasEndTime() != expected) {
            failWithMessage("Expected hasEndTime to be <%b> but was <%b>", expected, actual.hasEndTime());
        }

        return this;
    }

    public CrawlExecutionStatusAssert currentUriIdCountIsEqualTo(int expected) {
        isNotNull();
        if (actual.getCurrentUriIdCount() != expected) {
            failWithMessage("Expected currentUriIdCount to be <%s> but was <%s>", expected, actual.getCurrentUriIdCount());
        }

        return this;
    }

    public CrawlExecutionStatusAssert documentsCrawledEquals(long expected) {
        return checkCountEquals("documentsCrawled", actual.getDocumentsCrawled(), expected);
    }

    public CrawlExecutionStatusAssert documentsCrawledSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsCrawled", actual.getDocumentsCrawled(), condition);
    }

    public CrawlExecutionStatusAssert documentsDeniedEquals(long expected) {
        return checkCountEquals("documentsDenied", actual.getDocumentsDenied(), expected);
    }

    public CrawlExecutionStatusAssert documentsDeniedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsDenied", actual.getDocumentsDenied(), condition);
    }

    public CrawlExecutionStatusAssert documentsFailedEquals(long expected) {
        return checkCountEquals("documentsFailed", actual.getDocumentsFailed(), expected);
    }

    public CrawlExecutionStatusAssert documentsFailedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsFailed", actual.getDocumentsFailed(), condition);
    }

    public CrawlExecutionStatusAssert documentsRetriedEquals(long expected) {
        return checkCountEquals("documentsRetried", actual.getDocumentsRetried(), expected);
    }

    public CrawlExecutionStatusAssert documentsRetriedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsRetried", actual.getDocumentsRetried(), condition);
    }

    public CrawlExecutionStatusAssert documentsOutOfScopeEquals(long expected) {
        return checkCountEquals("documentsOutOfScope", actual.getDocumentsOutOfScope(), expected);
    }

    public CrawlExecutionStatusAssert documentsOutOfScopeSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsOutOfScope", actual.getDocumentsOutOfScope(), condition);
    }

    private CrawlExecutionStatusAssert checkCountEquals(String name, long val, long expected) {
        isNotNull();
        if (val != expected) {
            failWithMessage("Expected %s to be <%d> but was <%d>", name, expected, val);
        }
        return this;
    }

    private CrawlExecutionStatusAssert checkCountSatisfies(String name, long val, Consumer<LongAssert> condition) {
        isNotNull();
        condition.accept(new LongAssert(val).as(name));
        return this;
    }
}
