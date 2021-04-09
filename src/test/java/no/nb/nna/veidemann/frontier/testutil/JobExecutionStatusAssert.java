package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.IntegerAssert;
import org.assertj.core.api.LongAssert;

import java.util.Objects;
import java.util.function.Consumer;

public class JobExecutionStatusAssert extends AbstractAssert<JobExecutionStatusAssert, JobExecutionStatus> {

    public JobExecutionStatusAssert(JobExecutionStatus actual) {
        super(actual, JobExecutionStatusAssert.class);
    }

    public JobExecutionStatusAssert hasState(State expected) {
        isNotNull();
        if (!Objects.equals(actual.getState(), expected)) {
            failWithMessage("Expected state to be <%s> but was <%s>", expected, actual.getState());
        }

        return this;
    }

    public JobExecutionStatusAssert hasStartTime(boolean expected) {
        isNotNull();
        if (actual.hasStartTime() != expected) {
            failWithMessage("Expected hasStartTime to be <%b> but was <%b>", expected, actual.hasStartTime());
        }

        return this;
    }

    public JobExecutionStatusAssert hasEndTime(boolean expected) {
        isNotNull();
        if (actual.hasEndTime() != expected) {
            failWithMessage("Expected hasEndTime to be <%b> but was <%b>", expected, actual.hasEndTime());
        }

        return this;
    }

    public JobExecutionStatusAssert documentsCrawledEquals(long expected) {
        return checkCountEquals("documentsCrawled", actual.getDocumentsCrawled(), expected);
    }

    public JobExecutionStatusAssert documentsCrawledSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsCrawled", actual.getDocumentsCrawled(), condition);
    }

    public JobExecutionStatusAssert documentsDeniedEquals(long expected) {
        return checkCountEquals("documentsDenied", actual.getDocumentsDenied(), expected);
    }

    public JobExecutionStatusAssert documentsDeniedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsDenied", actual.getDocumentsDenied(), condition);
    }

    public JobExecutionStatusAssert documentsFailedEquals(long expected) {
        return checkCountEquals("documentsFailed", actual.getDocumentsFailed(), expected);
    }

    public JobExecutionStatusAssert documentsFailedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsFailed", actual.getDocumentsFailed(), condition);
    }

    public JobExecutionStatusAssert documentsRetriedEquals(long expected) {
        return checkCountEquals("documentsRetried", actual.getDocumentsRetried(), expected);
    }

    public JobExecutionStatusAssert documentsRetriedSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsRetried", actual.getDocumentsRetried(), condition);
    }

    public JobExecutionStatusAssert documentsOutOfScopeEquals(long expected) {
        return checkCountEquals("documentsOutOfScope", actual.getDocumentsOutOfScope(), expected);
    }

    public JobExecutionStatusAssert documentsOutOfScopeSatisfies(Consumer<LongAssert> condition) {
        return checkCountSatisfies("documentsOutOfScope", actual.getDocumentsOutOfScope(), condition);
    }

    public JobExecutionStatusAssert executionsStateCountEquals(CrawlExecutionStatus.State state, int expected) {
        isNotNull();
        int val = actual.getExecutionsStateOrDefault(state.name(), 0);
        if (val != expected) {
            failWithMessage("Expected %s count to be <%d> but was <%d>", state.name(), expected, val);
        }
        return this;
    }

    public JobExecutionStatusAssert executionsStateCountSatifies(CrawlExecutionStatus.State state, Consumer<IntegerAssert> condition) {
        isNotNull();
        int val = actual.getExecutionsStateOrDefault(state.name(), 0);
        condition.accept(new IntegerAssert(val).as(state.name()));
        return this;
    }

    private JobExecutionStatusAssert checkCountEquals(String name, long val, long expected) {
        isNotNull();
        if (val != expected) {
            failWithMessage("Expected %s to be <%d> but was <%d>", name, expected, val);
        }
        return this;
    }

    private JobExecutionStatusAssert checkCountSatisfies(String name, long val, Consumer<LongAssert> condition) {
        isNotNull();
        condition.accept(new LongAssert(val).as(name));
        return this;
    }
}
