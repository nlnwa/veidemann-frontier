package no.nb.nna.veidemann.frontier.db;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.CrawlableUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.frontier.FrontierService;
import no.nb.nna.veidemann.frontier.api.Context;
import no.nb.nna.veidemann.frontier.db.script.AddUriScript;
import no.nb.nna.veidemann.frontier.db.script.ChgNextScript;
import no.nb.nna.veidemann.frontier.db.script.ChgQueueCountScript;
import no.nb.nna.veidemann.frontier.db.script.ChgReleaseScript;
import no.nb.nna.veidemann.frontier.db.script.ChgUpdateBusyTimeoutScript;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript.NextUriScriptResult;
import no.nb.nna.veidemann.frontier.db.script.RedisJob.JedisContext;
import no.nb.nna.veidemann.frontier.db.script.RemoveUriScript;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.QueuedUriWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CrawlQueueManager implements AutoCloseable {
    public final static String CHG_BUSY_KEY = "chg_busy";
    public final static String CHG_READY_KEY = "chg_ready";
    public final static String CHG_WAIT_KEY = "chg_wait";
    public final static String CRAWL_EXECUTION_RUNNING_KEY = "ceid_running";
    public final static String CRAWL_EXECUTION_TIMEOUT_KEY = "ceid_timeout";
    private static final Logger LOG = LoggerFactory.getLogger(CrawlQueueManager.class);

    public static final String UEID = "UEID:";
    public static final String UCHG = "UCHG:";
    public static final String CRAWL_EXECUTION_ID_COUNT_KEY = "EIDC";
    public static final String CHG_PREFIX = "CHG:";
    public static final String QUEUE_COUNT_TOTAL_KEY = "QCT";
    public static final String REMOVE_URI_QUEUE_KEY = "REMURI";
    public static final String URI_ALREADY_INCLUDED_PREFIX = "AINC:";

    static final RethinkDB r = RethinkDB.r;
    public static final long RESCHEDULE_DELAY = 1000;

    private final RethinkDbConnection conn;
    private final JedisPool jedisPool;
    final AddUriScript addUriScript;
    final RemoveUriScript removeUriScript;
    final NextUriScript nextUriScript;
    final ChgNextScript getNextChgScript;
    final ChgReleaseScript releaseChgScript;
    final ChgQueueCountScript countChgScript;
    final ChgUpdateBusyTimeoutScript chgUpdateBusyTimeoutScript;

    private final CrawlQueueWorker chgManager;
    private final TimeoutSupplier<CrawlableUri> nextFetchSupplier;

    public CrawlQueueManager(Frontier frontier, RethinkDbConnection conn, JedisPool jedisPool) {
        this.conn = conn;
        this.jedisPool = jedisPool;
        addUriScript = new AddUriScript();
        removeUriScript = new RemoveUriScript();
        nextUriScript = new NextUriScript();
        getNextChgScript = new ChgNextScript();
        releaseChgScript = new ChgReleaseScript();
        countChgScript = new ChgQueueCountScript();
        chgUpdateBusyTimeoutScript = new ChgUpdateBusyTimeoutScript();

        this.chgManager = new CrawlQueueWorker(frontier, conn, jedisPool);
        this.nextFetchSupplier = new TimeoutSupplier<>(32, 15, TimeUnit.SECONDS, 4,
                () -> getNextToFetch(), u -> releaseCrawlHostGroup(u.getCrawlHostGroup(), RESCHEDULE_DELAY));
    }

    public static String createChgPolitenessKey(String chgId, String politenessId) {
        return chgId + ":" + politenessId;
    }

    public static String createChgPolitenessKey(QueuedUri qUri) {
        return createChgPolitenessKey(qUri.getCrawlHostGroupId(), qUri.getPolitenessRef().getId());
    }

    public static String createChgPolitenessKey(CrawlHostGroup chg) {
        return createChgPolitenessKey(chg.getId(), chg.getPolitenessId());
    }

    public QueuedUri addToCrawlHostGroup(QueuedUri qUri) throws DbException {
        Objects.requireNonNull(qUri.getCrawlHostGroupId(), "CrawlHostGroupId cannot be null");
        Objects.requireNonNull(qUri.getPolitenessRef().getId(), "PolitenessId cannot be null");
        if (qUri.getSequence() <= 0L) {
            throw new IllegalArgumentException("Sequence must be a positive number");
        }

        try {
            if (!qUri.hasEarliestFetchTimeStamp()) {
                qUri = qUri.toBuilder().setEarliestFetchTimeStamp(ProtoUtils.getNowTs()).build();
            }

            Map rMap = ProtoUtils.protoToRethink(qUri);

            // Ensure that the URI we are about to add is not present in remove queue.
            try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
                long c = ctx.getJedis().lrem(REMOVE_URI_QUEUE_KEY, 0, qUri.getId());
            }

            Map<String, Object> response = conn.exec("db-saveQueuedUri",
                    r.table(Tables.URI_QUEUE.name)
                            .insert(rMap)
                            .optArg("durability", "soft")
                            .optArg("conflict", "replace")
                            .optArg("return_changes", "always"));
            List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

            Map newDoc = changes.get(0).get("new_val");
            qUri = ProtoUtils.rethinkToProto(newDoc, QueuedUri.class);
            try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
                addUriScript.run(ctx, qUri);
            }
            return qUri;
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            return null;
        }
    }

    public CrawlableUri getNextToFetch(Context ctx) {
        while (!ctx.isCancelled()) {
            try {
                CrawlableUri u = nextFetchSupplier.get(1, TimeUnit.SECONDS);
                // Reset fetch timeout to compensate for idle time waiting in queue
                if (u != null && updateBusyTimeout(u.getCrawlHostGroup(),
                        Instant.now().plus(FrontierService.getSettings().getBusyTimeout()).toEpochMilli())) {
                    return u;
                }
            } catch (InterruptedException e) {
                return null;
            }
        }
        return null;
    }

    private CrawlableUri getNextToFetch() {
        try (JedisContext jedisContext = JedisContext.forPool(jedisPool)) {

            CrawlHostGroup chg = getNextReadyCrawlHostGroup(jedisContext);
            if (chg == null) {
                return null;
            }

            String chgId = createChgPolitenessKey(chg);
            LOG.trace("Found Crawl Host Group ({})", chgId);

            // try to find URI for CrawlHostGroup
            FutureOptional<QueuedUri> foqu = getNextQueuedUriToFetch(jedisContext, chg, conn);

            if (foqu.isPresent()) {
                LOG.debug("Found Queued URI: {}, crawlHostGroup: {}, sequence: {}",
                        foqu.get().getUri(), foqu.get().getCrawlHostGroupId(), foqu.get().getSequence());
                return new CrawlableUri(chg, foqu.get());
            } else if (foqu.isMaybeInFuture()) {
                // A URI was found, but isn't fetchable yet. Reschedule it.
                LOG.trace("Queued URI might be available at: {}", foqu.getWhen());

                // Pick delay time between ordinary RESCHEDULE_DELAY and when known uri is available.
                // This ensures new uri's do not have to wait until a failed uri is eligible for retry while
                // not using to much resources.
                long delay = (RESCHEDULE_DELAY + foqu.getDelayMs()) / 2;
                releaseCrawlHostGroup(jedisContext, chg, delay);
            } else {
                // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                LOG.warn("No Queued URI found waiting {}ms before retry", RESCHEDULE_DELAY);
                releaseCrawlHostGroup(jedisContext, chg, RESCHEDULE_DELAY);
            }
        } catch (Exception e) {
            LOG.error("Failed borrowing CrawlHostGroup", e);
        }

        return null;
    }

    public CrawlExecution createCrawlExecution(Context ctx, CrawlableUri cUri) throws DbException {
        if (cUri != null) {
            return new CrawlExecution(cUri.getUri(), cUri.getCrawlHostGroup(), ctx.getFrontier());
        } else {
            return null;
        }
    }

    public long deleteQueuedUrisForExecution(String executionId) throws DbException {
        try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
            return deleteQueuedUrisForExecution(ctx, executionId);
        }
    }

    public long deleteQueuedUrisForExecution(JedisContext ctx, String executionId) throws DbException {
        long deleted = 0;
        ScanResult<String> queues = ctx.getJedis().scan("0", new ScanParams().match(UEID + "*:" + executionId));
        while (!queues.isCompleteIteration()) {
            for (String queue : queues.getResult()) {
                String[] queueParts = queue.split(":");
                String chgp = queueParts[1] + ":" + queueParts[2];
                ScanResult<Tuple> uris = new ScanResult<Tuple>("0", null);
                do {
                    uris = ctx.getJedis().zscan(queue, uris.getCursor());
                    for (Tuple uri : uris.getResult()) {
                        String[] uriParts = uri.getElement().split(":", 3);
                        String uriId = uriParts[2];
                        long sequence = Longs.tryParse(uriParts[0].trim());
                        long fetchTime = Longs.tryParse(uriParts[1].trim());

                        // Remove queued uri from Redis
                        removeQUri(ctx, uriId, chgp, executionId, sequence, fetchTime, true);
                    }
                } while (!uris.isCompleteIteration());
            }
            queues = ctx.getJedis().scan(queues.getCursor(), new ScanParams().match(UEID + "*:" + executionId));
        }
        return deleted;
    }

    String key(String jobId) {
        return URI_ALREADY_INCLUDED_PREFIX + jobId;
    }

    /**
     * Atomically checks if a uri is already included in queue for a JobExecution and adds the uri
     * to the datastructure such that the next call to this function with the same QueuedUri will always return false.
     *
     * @param qu the uri to check
     * @return true if the uri is not seen for the JobExecution
     */
    public boolean uriNotIncludedInQueue(QueuedUriWrapper qu) {
        String jobExecutionId = qu.getJobExecutionId();
        String uriHash = uriHash(qu.getIncludedCheckUri());
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sadd(key(jobExecutionId), uriHash) == 1;
        }
    }

    /**
     * Resets the already included datastructure for a JobExecution.
     *
     * @param jobExecutionId
     */
    public void removeAlreadyIncludedQueue(String jobExecutionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key(jobExecutionId));
        }
    }

    private String uriHash(String uri) {
        return Hashing.sha256().hashUnencodedChars(uri).toString();
    }

    FutureOptional<QueuedUri> getNextQueuedUriToFetch(JedisContext ctx, CrawlHostGroup crawlHostGroup, RethinkDbConnection conn) {
        NextUriScriptResult res = nextUriScript.run(ctx, crawlHostGroup);
        if (res.future != null) {
            return res.future;
        }

        Map<String, Object> obj = null;
        try {
            obj = conn.exec("db-getNextQueuedUriToFetch",
                    r.table(Tables.URI_QUEUE.name)
                            .get(res.id));
        } catch (DbConnectionException e) {
            e.printStackTrace();
        } catch (DbQueryException e) {
            e.printStackTrace();
        }

        if (obj != null) {
            return FutureOptional.of(no.nb.nna.veidemann.db.ProtoUtils.rethinkToProto(obj, QueuedUri.class));
        } else {
            LOG.warn("Db inconsistency: Could not find queued uri: {}, CHG: {}", res.id, res.chgp);
            removeQUri(ctx, res.id, res.chgp, res.eid, res.sequence, res.fetchTime, false);
            return FutureOptional.empty();
        }
    }

    public long countByCrawlExecution(String executionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String c = jedis.hget(CRAWL_EXECUTION_ID_COUNT_KEY, executionId);
            if (c == null) {
                return 0;
            }
            return Longs.tryParse(c);
        }
    }

    public long countByCrawlHostGroup(CrawlHostGroup chg) {
        return countChgScript.run(JedisContext.forPool(jedisPool), chg);
    }

    public long queueCountTotal() {
        try (Jedis jedis = jedisPool.getResource()) {
            String c = jedis.get(QUEUE_COUNT_TOTAL_KEY);
            if (c == null) {
                return 0;
            }
            return Longs.tryParse(c);
        }
    }

    public long busyCrawlHostGroupCount() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(CHG_BUSY_KEY);
        }
    }

    private void removeQUri(JedisContext ctx, String id, String chgp, String eid, long sequence, long fetchTime, boolean deleteUri) {
        long numRemoved = removeUriScript.run(ctx, id, chgp, eid, sequence, fetchTime, deleteUri);
        if (numRemoved != 1) {
            LOG.debug("Queued uri id '{}' to be removed from Redis was not found", id);
        }
    }

    public boolean removeTmpCrawlHostGroup(QueuedUri qUri, String chg) {
        return removeQUri(qUri, chg, false);
    }

    public boolean removeQUri(QueuedUri qUri, String chg) {
        return removeQUri(qUri, chg, true);
    }

    private boolean removeQUri(QueuedUri qUri, String chg, boolean deleteUri) {
        if (LOG.isTraceEnabled()) {
            String stack = Arrays.stream(new RuntimeException().getStackTrace())
                    .filter(s -> s.getClassName().contains("no.nb.nna"))
                    .map(s -> String.format("%s:%s(%d)", s.getClassName().substring(s.getClassName().lastIndexOf(".") + 1), s.getMethodName(), s.getLineNumber()))
                    .filter(s -> !s.equals("CrawlQueueManager:removeQUri(351)"))
                    .reduce("", (r, s) -> r == "" ? s : r + "<<" + s);
            LOG.trace("remUri: {}, Trace: {}", qUri.getId(), stack);
        }

        String chgp = createChgPolitenessKey(chg, qUri.getPolitenessRef().getId());
        try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
            long numRemoved = removeUriScript.run(ctx,
                    qUri.getId(),
                    chgp,
                    qUri.getExecutionId(),
                    qUri.getSequence(),
                    qUri.getEarliestFetchTimeStamp().getSeconds(),
                    deleteUri);
            if (numRemoved != 1) {
                LOG.debug("Queued uri id '{}' to be removed from Redis was not found", qUri.getId());
            }
        }
        return true;
    }

    public Long getBusyTimeout(CrawlHostGroup crawlHostGroup) {
        try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
            Double timeout = ctx.getJedis().zscore(CHG_BUSY_KEY, createChgPolitenessKey(crawlHostGroup));
            if (timeout == null) {
                return null;
            }
            return timeout.longValue();
        }
    }

    /**
     * Update timeout for busy CHG.
     * <p>
     * Timeout is only updated if CHG is already in busy state. If CHG was not busy, nothing is done and the return value is false.
     *
     * @param crawlHostGroup the CHG to update
     * @param timeoutMs      the new timeout value
     * @return true if CHG was busy
     */
    public boolean updateBusyTimeout(CrawlHostGroup crawlHostGroup, Long timeoutMs) {
        try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
            return updateBusyTimeout(ctx, crawlHostGroup, timeoutMs);
        }
    }

    public boolean updateBusyTimeout(JedisContext ctx, CrawlHostGroup crawlHostGroup, Long timeoutMs) {
        Long resp = chgUpdateBusyTimeoutScript.run(ctx, createChgPolitenessKey(crawlHostGroup), timeoutMs);
        return resp != null;
    }

    private CrawlHostGroup getNextReadyCrawlHostGroup(JedisContext jedisContext) {
        try {
            long busyTimeout = FrontierService.getSettings().getBusyTimeout().toMillis();
            return getNextChgScript.run(jedisContext, busyTimeout);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            return null;
        }
    }

    public void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) {
        try (JedisContext ctx = JedisContext.forPool(jedisPool)) {
            releaseCrawlHostGroup(ctx, crawlHostGroup, nextFetchDelayMs);
        }
    }

    public void releaseCrawlHostGroup(JedisContext ctx, CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) {
        // Ensure CHG is busy before releasing
        if (updateBusyTimeout(ctx, crawlHostGroup, System.currentTimeMillis() + 500)) {
            LOG.debug("Releasing CrawlHostGroup: {}, with queue count: {}", crawlHostGroup.getId(), crawlHostGroup.getQueuedUriCount());
            releaseChgScript.run(ctx, crawlHostGroup, nextFetchDelayMs);
        }
    }

    public void scheduleCrawlExecutionTimeout(String ceid, OffsetDateTime timeout) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zadd(CRAWL_EXECUTION_RUNNING_KEY, timeout.toInstant().toEpochMilli(), ceid);
        }
    }

    public void removeCrawlExecutionFromTimeoutSchedule(String executionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zrem(CRAWL_EXECUTION_RUNNING_KEY, executionId);
        }
    }

    public void pause(boolean pause) {
        nextFetchSupplier.pause(pause);
    }

    @Override
    public void close() throws InterruptedException {
        nextFetchSupplier.close();
        chgManager.close();
    }
}
