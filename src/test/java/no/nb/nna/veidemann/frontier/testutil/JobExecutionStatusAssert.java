package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;

import java.util.Objects;

public class JobExecutionStatusAssert extends IdMappedAssert.FromMapAssert<JobExecutionStatusAssert, JobExecutionStatus> {

    public JobExecutionStatusAssert(IdMappedAssert origin, JobExecutionStatus actual) {
        super(origin, actual);
    }

    public JobExecutionStatusAssert hasState(State expected) {
        isNotNull();
        if (!Objects.equals(actual.getState(), expected)) {
            failWithMessage("Expected state to be <%s> but was <%s>", expected, actual.getState());
        }

        return this;
    }

    public JobExecutionStatusAssert hasStats(long documentsCrawled, long documentsDenied, long documentsFailed,
                                             long documentsRetried, long documentsOutOfScope) {
        isNotNull();
        if (actual.getDocumentsCrawled() != documentsCrawled) {
            failWithMessage("Expected documentsCrawled to be <%d> but was <%d>", documentsCrawled, actual.getDocumentsCrawled());
        }
        if (actual.getDocumentsDenied() != documentsDenied) {
            failWithMessage("Expected documentsDenied to be <%d> but was <%d>", documentsDenied, actual.getDocumentsDenied());
        }
        if (actual.getDocumentsFailed() != documentsFailed) {
            failWithMessage("Expected documentsFailed to be <%d> but was <%d>", documentsFailed, actual.getDocumentsFailed());
        }
        if (actual.getDocumentsRetried() != documentsRetried) {
            failWithMessage("Expected documentsRetried to be <%d> but was <%d>", documentsRetried, actual.getDocumentsRetried());
        }
        if (actual.getDocumentsOutOfScope() != documentsOutOfScope) {
            failWithMessage("Expected documentsOutOfScope to be <%d> but was <%d>", documentsOutOfScope, actual.getDocumentsOutOfScope());
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
}
