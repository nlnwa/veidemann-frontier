package no.nb.nna.veidemann.frontier.testutil;

import org.assertj.core.api.AbstractAssert;

public class RequestLogAssert extends AbstractAssert<RequestLogAssert, RequestLog> {
    public RequestLogAssert(RequestLog actual) {
        super(actual, RequestLogAssert.class);
    }

    public static RequestLogAssert assertThat(RequestLog actual) {
        return new RequestLogAssert(actual);
    }

    public RequestLogAssert hasNumberOfRequests(String url, int count) {
        isNotNull();
        if (actual.getCount(url) != count) {
            failWithMessage("Expected <%d> number of requests for <%s>, but was <%d>", count, url, actual.getCount(url));
        }
        return this;
    }
}
