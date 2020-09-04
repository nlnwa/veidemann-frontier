package no.nb.nna.veidemann.frontier.api;

import io.grpc.stub.ServerCallStreamObserver;
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
    private final AtomicInteger amountOfActivePageFetchesCounter;

    static final Lock lock = new ReentrantLock();
    static final Condition notTerminated = lock.newCondition();

    public Context() {
        isShutdown = new AtomicBoolean(false);
        amountOfActiveObserversCounter = new AtomicInteger(0);
        amountOfActivePageFetchesCounter = new AtomicInteger(0);
    }

    public RequestContext newRequestContext(ServerCallStreamObserver responseObserver) {
        return new RequestContext(responseObserver);
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

    public class RequestContext extends Context {
        private final ServerCallStreamObserver responseObserver;
        private final AtomicBoolean observerCompleted = new AtomicBoolean(false);
        private final AtomicBoolean pageFetchStarted = new AtomicBoolean(false);

        private RequestContext(ServerCallStreamObserver responseObserver) {
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
                LOG.trace("Client disconnected. Currently active clients: {}. Currently active page fetches: {}", amountOfActiveObserversCounter.get(), amountOfActivePageFetchesCounter.get());
            }
        }
    }
}
