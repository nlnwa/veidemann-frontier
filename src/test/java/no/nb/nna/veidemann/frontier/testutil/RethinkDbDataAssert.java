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

    public RethinkDbDataAssert jobStatsMatchesCrawlExecutions() throws DbQueryException, DbConnectionException {
        for (JobExecutionStatus jb : actual.getJobExecutionStatuses().values()) {
            JobExecutionStatus.Builder jobExecution = jb.toBuilder();
            jb.getExecutionsStateMap().entrySet().stream().filter(e -> e.getValue() == 0).forEach(e -> jobExecution.removeExecutionsState(e.getKey()));
            jobExecution.clearId().clearJobId().clearStartTime().clearEndTime().clearState().clearDesiredState();

            JobExecutionStatus.Builder computedJob = actual.getCrawlExecutionStatuses().values().stream()
                    .filter(e -> e.getJobExecutionId().equals(jb.getId()))
                    .map(e -> JobExecutionStatus.newBuilder()
                            .setDocumentsCrawled(e.getDocumentsCrawled())
                            .setDocumentsDenied(e.getDocumentsDenied())
                            .setDocumentsFailed(e.getDocumentsFailed())
                            .setDocumentsOutOfScope(e.getDocumentsOutOfScope())
                            .setDocumentsRetried(e.getDocumentsRetried())
                            .setBytesCrawled(e.getBytesCrawled())
                            .setUrisCrawled(e.getUrisCrawled())
                            .putExecutionsState(e.getState().name(), 1)
                    )
                    .reduce(JobExecutionStatus.newBuilder(),
                            (result, j) -> {
                                result.setDocumentsCrawled(result.getDocumentsCrawled() + j.getDocumentsCrawled())
                                        .setDocumentsDenied(result.getDocumentsDenied() + j.getDocumentsDenied())
                                        .setDocumentsFailed(result.getDocumentsFailed() + j.getDocumentsFailed())
                                        .setDocumentsOutOfScope(result.getDocumentsOutOfScope() + j.getDocumentsOutOfScope())
                                        .setDocumentsRetried(result.getDocumentsRetried() + j.getDocumentsRetried())
                                        .setBytesCrawled(result.getBytesCrawled() + j.getBytesCrawled())
                                        .setUrisCrawled(result.getUrisCrawled() + j.getUrisCrawled());
                                j.getExecutionsStateMap().forEach((key, value) -> {
                                    result.putExecutionsState(key, result.getExecutionsStateOrDefault(key, 0) + value);
                                });
                                return result;
                            }
                    );
            if (!jobExecution.build().equals(computedJob.build())) {
                failWithMessage("Expected sum of all stats from crawl executions to match job execution, but did not.\n" +
                        "Calculated:\n<%s>\nActual:\n<%s>", computedJob, jobExecution);
            }
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
