package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus.State;

import java.util.Objects;

public class CrawlExecutionStatusAssert extends IdMappedAssert.FromMapAssert<CrawlExecutionStatusAssert, CrawlExecutionStatus> {

    public CrawlExecutionStatusAssert(IdMappedAssert origin, CrawlExecutionStatus actual) {
        super(origin, actual);
    }

    public CrawlExecutionStatusAssert hasState(State expected) {
        isNotNull();
        if (!Objects.equals(actual.getState(), expected)) {
            failWithMessage("Expected state to be <%s> but was <%s>", expected, actual.getState());
        }

        return this;
    }

    public CrawlExecutionStatusAssert hasStats(long documentsCrawled, long documentsDenied, long documentsFailed,
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

//    public CrawlExecutionStatusAssert next() {
//        return new CrawlExecutionStatusAssert(statusIterator);
//    }

//    public CrawlExecutionStatusAssert hasCrawlExecutionStatuses(EssentialQueuedUriFields... expected) {
//        isNotNull();
//
//        List<EssentialQueuedUriFields> actualEessentialQueuedUriFields = actual.queuedUriList.stream()
//                .map(qUri -> new EssentialQueuedUriFields(qUri.getUri(), qUri.getSeedUri(), qUri.getIp(), !qUri.getCrawlHostGroupId().isBlank()))
//                .collect(Collectors.toList());
//
//        for (EssentialQueuedUriFields e : expected) {
//            if (!actualEessentialQueuedUriFields.contains(e)) {
//                failWithMessage("Expected Queued Uri's to contain <%s> but did not", e);
//            }
//        }
//
//        return this;
//    }
}
