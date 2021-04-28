package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.log.v1.CrawlLog;

public class FrontierAssertions {
    public static <T> RequestLogAssert<T> assertThat(RequestLog<T> actual) {
        return new RequestLogAssert<T>(actual);
    }

    public static RethinkDbDataAssert assertThat(RethinkDbData actual) {
        return new RethinkDbDataAssert(actual);
    }

    public static RedisDataAssert assertThat(RedisData actual) {
        return new RedisDataAssert(actual);
    }

    public static CrawlHostGroupAssert assertThat(CrawlHostGroup actual) {
        return new CrawlHostGroupAssert(actual);
    }

    public static JobExecutionStatusAssert assertThat(JobExecutionStatus actual) {
        return new JobExecutionStatusAssert(actual);
    }

    public static CrawlExecutionStatusAssert assertThat(CrawlExecutionStatus actual) {
        return new CrawlExecutionStatusAssert(actual);
    }

    public static CrawlLogAssert assertThat(CrawlLog actual) {
        return new CrawlLogAssert(actual);
    }

    public static ErrorAssert assertThat(Error actual) {
        return new ErrorAssert(actual);
    }

}
