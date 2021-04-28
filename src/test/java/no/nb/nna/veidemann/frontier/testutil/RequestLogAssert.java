package no.nb.nna.veidemann.frontier.testutil;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.ListAssert;
import org.assertj.core.internal.Iterables;

import java.util.function.Consumer;

public class RequestLogAssert<T> extends AbstractAssert<RequestLogAssert<T>, RequestLog<T>> {
    public RequestLogAssert(RequestLog<T> actual) {
        super(actual, RequestLogAssert.class);
    }

    public static <T> RequestLogAssert<T> assertThat(RequestLog<T> actual) {
        return new RequestLogAssert<T>(actual);
    }

    public RequestLogAssert<T> hasNumberOfRequests(int count) {
        isNotNull();
        if (actual.getTotalCount() != count) {
            failWithMessage("Expected <%d> number of requests, but was <%d>", count, actual.getTotalCount());
        }
        return this;
    }

    public RequestLogAssert<T> hasNumberOfRequests(T value, int count) {
        isNotNull();
        if (actual.getCount(value) != count) {
            failWithMessage("Expected <%d> number of requests for <%s>, but was <%d>", count, value, actual.getCount(value));
        }
        return this;
    }

    public RequestLogAssert<T> hasRequestSatisfying(Consumer<T> requirements) {
        isNotNull();
        Iterables.instance().assertAnySatisfy(info, actual.requests, requirements);
        return this;
    }

    public ListAssert<T> requests() {
        return new ListAssert<T>(actual.requests);
    }
}
