package no.nb.nna.veidemann.frontier.testutil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestLog {
    HashMap<String, Integer> log = new HashMap<>();
    Lock lock = new ReentrantLock();

    public void addRequest(String value) {
        lock.lock();
        try {
            log.merge(value, 1, (x, y) -> x + y);
        } finally {
            lock.unlock();
        }
    }

    public int getTotalCount() {
        return log.values().stream().reduce(0, Integer::sum);
    }

    public int getUniqueCount() {
        return log.size();
    }

    public int getCount(String value) {
        return log.getOrDefault(value, 0);
    }

    public Map<Integer, Integer> getHistogram() {
        Map<Integer, Integer> h = new HashMap<>();
        log.values().forEach(v -> h.merge(v, 1, (x, y) -> x + y));
        return h;
    }
}
