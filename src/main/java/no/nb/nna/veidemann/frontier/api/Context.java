package no.nb.nna.veidemann.frontier.api;

import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Context {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
    private final AtomicBoolean isShutdown;
    private final AtomicInteger amountOfActiveObserversCounter;
    private final Frontier frontier;

    static final Lock lock = new ReentrantLock();
    static final Condition notTerminated = lock.newCondition();

    public Context(Frontier frontier) {
        isShutdown = new AtomicBoolean(false);
        amountOfActiveObserversCounter = new AtomicInteger(0);
        this.frontier = frontier;
    }

    public void shutdown() {
        isShutdown.set(true);
        if (amountOfActiveObserversCounter.get() <= 0) {
            lock.lock();
            try {
                notTerminated.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * @param timeout
     * @param unit
     * @return true if this StreamObserverPool terminated and false if the timeout elapsed before termination
     * @throws InterruptedException
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (amountOfActiveObserversCounter.get() <= 0 && isShutdown.get()) {
            return true;
        }
        lock.lock();
        try {
            return notTerminated.await(timeout, unit);
        } finally {
            lock.unlock();
        }
    }

    public void awaitTermination() throws InterruptedException {
        if (amountOfActiveObserversCounter.get() <= 0 && isShutdown.get()) {
            return;
        }
        lock.lock();
        try {
            notTerminated.await();
        } finally {
            lock.unlock();
        }
    }

    public Frontier getFrontier() {
        return frontier;
    }

    public CrawlQueueManager getCrawlQueueManager() {
        return frontier.getCrawlQueueManager();
    }

    public void startPageComplete() {
        amountOfActiveObserversCounter.incrementAndGet();
        LOG.trace("Client connected. Currently active clients: {}", amountOfActiveObserversCounter.get());
    }

    public void setObserverCompleted() {
        if (amountOfActiveObserversCounter.decrementAndGet() <= 0 && isShutdown.get()) {
            lock.lock();
            try {
                notTerminated.signalAll();
            } finally {
                lock.unlock();
            }
        }
        LOG.trace("Client disconnected. Currently active clients: {}.",
                amountOfActiveObserversCounter.get());
    }
}
