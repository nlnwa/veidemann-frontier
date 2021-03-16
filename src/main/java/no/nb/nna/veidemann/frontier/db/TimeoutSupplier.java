package no.nb.nna.veidemann.frontier.db;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TimeoutSupplier<E> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TimeoutSupplier.class);

    private final int queueCapacity;
    private final Supplier<E> supplier;
    private final Consumer<E> timeoutHandler;
    private final long timeout;
    private final TimeUnit unit;
    private final ScheduledThreadPoolExecutor timeoutThread;
    private final ExecutorService supplierThread;
    private final Queue<Element> queue;
    private boolean paused = false;
    private boolean running = true;
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition mightHaveSpace = lock.newCondition();
    private int count;

    public TimeoutSupplier(int capacity, long timeout, TimeUnit unit, Supplier<E> supplier) {
        this(capacity, timeout, unit, 2, supplier, null);
    }

    public TimeoutSupplier(int capacity, long timeout, TimeUnit unit, Supplier<E> supplier, Consumer<E> timeoutHandler) {
        this(capacity, timeout, unit, 2, supplier, timeoutHandler);
    }

    public TimeoutSupplier(int capacity, long timeout, TimeUnit unit, int workerThreads, Supplier<E> supplier, Consumer<E> timeoutHandler) {
        this.queueCapacity = capacity;
        queue = new ArrayDeque<>(queueCapacity);
        this.supplier = supplier;
        this.timeoutHandler = timeoutHandler;
        this.timeout = timeout;
        this.unit = unit;

        timeoutThread = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("TimeoutSupplierCleaner-%d").build());
        timeoutThread.setRemoveOnCancelPolicy(true);

        supplierThread = Executors.newFixedThreadPool(workerThreads,
                new ThreadFactoryBuilder().setNameFormat("TimeoutSupplierWorker-%d").build());

        for (int i = 0; i < workerThreads; i++) {
            supplierThread.submit(() -> run());
        }
    }

    private void run() {
        while (true) {
            lock.lock();
            try {
                while (running && (paused || queue.size() >= queueCapacity)) {
                    mightHaveSpace.await();
                }
                if (!running) {
                    return;
                }
                count++;
            } catch (InterruptedException interruptedException) {
                return;
            } finally {
                lock.unlock();
            }

            Element e = null;
            try {
                E v = supplier.get();
                if (v != null) {
                    e = new Element(v);
                }
            } catch (Throwable t) {
                LOG.warn("Error thrown by supplier function", t);
                if (e != null) {
                    e.cancel();
                    continue;
                }
            }

            lock.lock();
            try {
                if (e == null) {
                    count--;
                    mightHaveSpace.signalAll();
                    continue;
                }

                if (!running) {
                    e.cancel();
                    return;
                }

                Element t = e;
                ScheduledFuture<?> f = timeoutThread.schedule(() -> t.cancel(), timeout, unit);
                e.setTimeoutFuture(f);
                if (queue.offer(t)) {
                    notEmpty.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void pause(boolean pause) {
        lock.lock();
        try {
            if (this.paused != pause) {
                this.paused = pause;
                mightHaveSpace.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public E get(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        E v = null;
        try {
            while (v == null) {
                Element e = queue.poll();
                while (e == null) {
                    if (nanos <= 0L)
                        return null;
                    nanos = notEmpty.awaitNanos(nanos);
                    e = queue.poll();
                }
                v = e.get();
                count--;
                mightHaveSpace.signal();
            }
            return v;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        LOG.debug("Closing TimeoutSupplier");
        lock.lock();
        try {
            running = false;
            while (!queue.isEmpty()) {
                queue.peek().cancel();
            }
        } finally {
            lock.unlock();
        }
        supplierThread.shutdown();
        timeoutThread.shutdown();
        supplierThread.awaitTermination(5, TimeUnit.SECONDS);
        timeoutThread.awaitTermination(5, TimeUnit.SECONDS);
        LOG.debug("TimeoutSupplier closed");
    }

    private class Element {
        private ScheduledFuture<?> timeOutFuture;
        private final E value;
        private boolean done = false;

        public Element(E value) {
            this.value = value;
        }

        public void setTimeoutFuture(ScheduledFuture<?> timeOutFuture) {
            this.timeOutFuture = timeOutFuture;
        }

        public void cancel() {
            lock.lock();
            try {
                if (!done) {
                    if (timeOutFuture != null) {
                        timeOutFuture.cancel(true);
                    }
                    queue.remove(this);
                    if (timeoutHandler != null) {
                        timeoutHandler.accept(value);
                    }
                    done = true;
                    count--;
                    mightHaveSpace.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        public E get() {
            lock.lock();
            try {
                if (done) {
                    return null;
                }
                timeOutFuture.cancel(false);
                done = true;
                return value;
            } finally {
                lock.unlock();
            }
        }
    }

    private class TimeoutHandler implements Runnable {
        final Element element;

        public TimeoutHandler(Element element) {
            this.element = element;
        }

        @Override
        public void run() {
            element.cancel();
        }
    }
}
