package no.nb.nna.veidemann.frontier.testutil;

import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;

public class FrontierAssertions {
    public static RequestLogAssert assertThat(RequestLog actual) {
        return new RequestLogAssert(actual);
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

}
