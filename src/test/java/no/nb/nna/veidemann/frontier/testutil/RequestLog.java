package no.nb.nna.veidemann.frontier.testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class RequestLog<T> {
    List<T> requests = new ArrayList<>();
    HashMap<T, Integer> uniqueRequests = new HashMap<>();
    Lock lock = new ReentrantLock();

    public void addRequest(T value) {
        lock.lock();
        try {
            requests.add(value);
            uniqueRequests.merge(value, 1, (x, y) -> x + y);
        } finally {
            lock.unlock();
        }
    }

    public int getTotalCount() {
        return requests.size();
    }

    public int getUniqueCount() {
        return uniqueRequests.size();
    }

    public int getCount(T value) {
        return uniqueRequests.getOrDefault(value, 0);
    }

    public T get(int index) {
        return requests.get(index);
    }

    public Stream<T> stream() {
        return requests.stream();
    }

    public Map<Integer, Integer> getHistogram() {
        Map<Integer, Integer> h = new HashMap<>();
        uniqueRequests.values().forEach(v -> h.merge(v, 1, (x, y) -> x + y));
        return h;
    }
}
