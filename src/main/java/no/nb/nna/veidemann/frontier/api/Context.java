package no.nb.nna.veidemann.frontier.api;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
    private final AtomicInteger amountOfActivePageFetchesCounter;
    private final Frontier frontier;

    static final Lock lock = new ReentrantLock();
    static final Condition notTerminated = lock.newCondition();

    static final ScheduledExecutorService timoutThread = Executors.newScheduledThreadPool(
            1, new ThreadFactoryBuilder().setNameFormat("fetch-timeout-%d").build());

    public Context(Frontier frontier) {
        isShutdown = new AtomicBoolean(false);
        amountOfActiveObserversCounter = new AtomicInteger(0);
        amountOfActivePageFetchesCounter = new AtomicInteger(0);
        this.frontier = frontier;
    }

    public RequestContext newRequestContext(ServerCallStreamObserver responseObserver) {
        return new RequestContext(frontier, responseObserver);
    }

    public boolean isCancelled() {
        return isShutdown.get();
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

    public int getActivePageFetchCount() {
        return amountOfActivePageFetchesCounter.get();
    }

    public CrawlQueueManager getCrawlQueueManager() {
        return frontier.getCrawlQueueManager();
    }

    public class RequestContext extends Context {
        private final ServerCallStreamObserver responseObserver;
        private final AtomicBoolean observerCompleted = new AtomicBoolean(false);
        private final AtomicBoolean pageFetchStarted = new AtomicBoolean(false);
        private final AtomicBoolean fetchReturned = new AtomicBoolean(false);
        private ScheduledFuture<Void> timeout;

        private RequestContext(Frontier frontier, ServerCallStreamObserver responseObserver) {
            super(frontier);
            this.responseObserver = responseObserver;
            amountOfActiveObserversCounter.incrementAndGet();
            LOG.trace("Client connected. Currently active clients: {}", amountOfActiveObserversCounter.get());
        }

        public boolean isCancelled() {
            return isShutdown.get() || responseObserver.isCancelled();
        }

        public ServerCallStreamObserver getResponseObserver() {
            return responseObserver;
        }

        public void startPageFetch() {
            if (pageFetchStarted.compareAndSet(false, true)) {
                amountOfActivePageFetchesCounter.incrementAndGet();
                LOG.trace("Page fetch started. Currently active page fetches: {}", amountOfActivePageFetchesCounter.get());
            }
        }

        public void setObserverCompleted() {
            if (observerCompleted.compareAndSet(false, true)) {
                if (pageFetchStarted.get()) {
                    amountOfActivePageFetchesCounter.decrementAndGet();
                }
                if (amountOfActiveObserversCounter.decrementAndGet() <= 0 && isShutdown.get()) {
                    lock.lock();
                    try {
                        notTerminated.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
                LOG.trace("Client disconnected. Currently active clients: {}. Currently active page fetches: {}",
                        amountOfActiveObserversCounter.get(), amountOfActivePageFetchesCounter.get());
            }
        }

        public void setFetchCompleted() {
            if (timeout != null) {
                // Stop the timeout cancel handler
                timeout.cancel(false);
            }
        }
    }
}
