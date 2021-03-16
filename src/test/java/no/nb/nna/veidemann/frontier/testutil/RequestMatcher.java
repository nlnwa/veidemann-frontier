package no.nb.nna.veidemann.frontier.testutil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RequestMatcher {
    final RequestLog requestLog;
    final Map<String, Set<Integer>> matchers = new HashMap<>();

    public RequestMatcher(RequestLog requestLog) {
        this.requestLog = requestLog;
    }

    public RequestMatcher withMatchAllRequests(String value) {
        matchers.put(value, new HashSet<>());
        return this;
    }

    public RequestMatcher withMatchRequests(String value, int from, int to) {
        Set<Integer> requestNumbers = new HashSet<>();
        for (int i = from; i <= to; i++) {
            requestNumbers.add(i);
        }
        matchers.merge(value, requestNumbers, (oldRequestNumbers, newRequestNumbers) -> {
            oldRequestNumbers.addAll(newRequestNumbers);
            return oldRequestNumbers;
        });
        return this;
    }

    public boolean match(String value) {
        Set<Integer> matcher = matchers.get(value);
        if (matcher == null) {
            return false;
        }
        if (matcher.isEmpty()) {
            return true;
        }
        if (matcher.contains(requestLog.getCount(value))) {
            return true;
        }
        return false;
    }
}
