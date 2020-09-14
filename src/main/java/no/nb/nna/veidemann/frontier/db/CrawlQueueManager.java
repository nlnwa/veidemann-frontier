package no.nb.nna.veidemann.frontier.db;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.CrawlableUri;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.FutureOptional;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.frontier.api.Context;
import no.nb.nna.veidemann.frontier.db.script.AddUriScript;
import no.nb.nna.veidemann.frontier.db.script.ChgNextScript;
import no.nb.nna.veidemann.frontier.db.script.ChgReleaseScript;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript;
import no.nb.nna.veidemann.frontier.db.script.NextUriScript.NextUriScriptResult;
import no.nb.nna.veidemann.frontier.db.script.RemoveUriScript;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.MILLIS;

public class CrawlQueueManager {
    public final static String CHG_BUSY_KEY = "chg_busy";
    public final static String CHG_READY_KEY = "chg_ready";
    public final static String CHG_WAIT_KEY = "chg_wait";
    public final static byte[] NEXT_FETCH_TIME_FIELD = "n".getBytes();
    public final static byte[] URI_COUNT_FIELD = "c".getBytes();
    private static final Logger LOG = LoggerFactory.getLogger(CrawlQueueManager.class);

    public static final String UEID = "UEID:";
    public static final String UCHG = "UCHG:";
    public static final String EIDC = "EIDC";
    public static final String CHG_PREFIX = "CHG:";
    public static final String QUEUE_COUNT_TOTAL_KEY = "QCT";
    public static final String REMOVE_URI_QUEUE_KEY = "REMURI";
    public static final String URI_ALREADY_INCLUDED_PREFIX = "AINC:";

    static final RethinkDB r = RethinkDB.r;
    private final static long BUSY_TIMEOUT = 300 * 1000;
    public static final long RESCHEDULE_DELAY = 1000;

    private final RethinkDbConnection conn;
    private final JedisPool jedisPool;
    final AddUriScript addUriScript;
    final RemoveUriScript removeUriScript;
    final NextUriScript nextUriScript;
    final ChgNextScript getNextChgScript;
    final ChgReleaseScript releaseChgScript;

    private final CrawlQueueWorker chgManager;

    public CrawlQueueManager(RethinkDbConnection conn, JedisPool jedisPool) {
        this.conn = conn;
        this.jedisPool = jedisPool;
        addUriScript = new AddUriScript(jedisPool);
        removeUriScript = new RemoveUriScript(jedisPool);
        nextUriScript = new NextUriScript(jedisPool);
        getNextChgScript = new ChgNextScript(jedisPool);
        releaseChgScript = new ChgReleaseScript(jedisPool);

        this.chgManager = new CrawlQueueWorker(conn, jedisPool);
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

    public static Map<byte[], byte[]> serializeCrawlHostGroup(CrawlHostGroup chg) {
        return ImmutableMap.of(NEXT_FETCH_TIME_FIELD, chg.getNextFetchTime().toByteArray());
    }

    public static CrawlHostGroup deserializeCrawlHostGroup(String id, List<byte[]> data) {
        id = id.replace(CHG_PREFIX, "");
        String[] idParts = id.split(":", 2);
        CrawlHostGroup.Builder chg = CrawlHostGroup.newBuilder()
                .setId(idParts[0])
                .setPolitenessId(idParts[1]);
        for (int i = 0; i < data.size(); i = i + 2) {
            if (Arrays.equals(NEXT_FETCH_TIME_FIELD, data.get(i))) {
                try {
                    chg.setNextFetchTime(Timestamp.parseFrom(data.get(i + 1)));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            } else if (Arrays.equals(URI_COUNT_FIELD, data.get(i))) {
                Long count = Longs.tryParse(new String(data.get(i + 1)));
                if (count != null) {
                    chg.setQueuedUriCount(count);
                }
            }
        }
        return chg.build();
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

            Map<String, Object> response = conn.exec("db-saveQueuedUri",
                    r.table(Tables.URI_QUEUE.name)
                            .insert(rMap)
                            .optArg("conflict", "replace")
                            .optArg("return_changes", "always"));
            List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

            Map newDoc = changes.get(0).get("new_val");
            qUri = ProtoUtils.rethinkToProto(newDoc, QueuedUri.class);
            addUriScript.run(qUri);
            return qUri;
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            return null;
        }
    }

    public CrawlableUri getNextToFetch(Context ctx) {
        try {
            if (DbService.getInstance().getExecutionsAdapter().getDesiredPausedState()) {
                Thread.sleep(RESCHEDULE_DELAY);
                return null;
            }

            CrawlHostGroup chg = getNextReadyCrawlHostGroup(ctx);
            if (chg == null) {
                return null;
            }
            if (ctx.isCancelled()) {
                releaseCrawlHostGroup(chg, RESCHEDULE_DELAY);
                return null;
            }

            String chgId = createChgPolitenessKey(chg);
            LOG.trace("Found Crawl Host Group ({})", chgId);

            // try to find URI for CrawlHostGroup
            FutureOptional<QueuedUri> foqu = getNextQueuedUriToFetch(chg, conn);

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
                releaseCrawlHostGroup(chg, delay);
            } else {
                // No URI found for this CrawlHostGroup. Wait for RESCHEDULE_DELAY and try again.
                LOG.trace("No Queued URI found waiting {}ms before retry", RESCHEDULE_DELAY);
                releaseCrawlHostGroup(chg, RESCHEDULE_DELAY);
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
        // TODO: Remove debug statements when this is tested
        long deleted = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            ScanResult<String> queues = jedis.scan("0", new ScanParams().match(UEID + "*:" + executionId));
            while (!queues.isCompleteIteration()) {
                for (String queue : queues.getResult()) {
                    System.out.println("QQ1: " + queue);
                    String chgp = queue.split(":")[1];
                    ScanResult<Tuple> uris = new ScanResult<Tuple>("0", null);
                    do {
                        uris = jedis.zscan(queue, uris.getCursor());
                        for (Tuple uri : uris.getResult()) {
                            System.out.println("QQ2: " + uri.getElement());
                            String[] uriParts = uri.getElement().split(":", 3);
                            String uriId = uriParts[2];
                            long sequence = Longs.tryParse(uriParts[0].trim());
                            long fetchTime = Longs.tryParse(uriParts[1].trim());

                            // Remove queued uri from Redis
                            removeQUri(uriId, chgp, executionId, sequence, fetchTime, true);
                        }
                    } while (!uris.isCompleteIteration());
                }
                queues = jedis.scan(queues.getCursor(), new ScanParams().match(UEID + "*:" + executionId));
            }
        }
        return deleted;
    }

    String key(String jobId) {
        return URI_ALREADY_INCLUDED_PREFIX + jobId;
    }

    public boolean uriNotIncludedInQueue(QueuedUri qu) {
        String jobExecutionId = qu.getJobExecutionId();
        String uriHash = uriHash(qu.getSurt());
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sadd(key(jobExecutionId), uriHash) == 1;
        }
    }

    public void removeAlreadyIncludedQueue(String jobExecutionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key(jobExecutionId));
        }
    }

    private String uriHash(String uri) {
        return Hashing.sha256().hashUnencodedChars(uri).toString();
    }

    FutureOptional<QueuedUri> getNextQueuedUriToFetch(CrawlHostGroup crawlHostGroup, RethinkDbConnection conn) {
        NextUriScriptResult res = nextUriScript.run(crawlHostGroup);
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
            LOG.warn("Db inconsistency: Could not find queued uri: {}", res.id);
            removeQUri(res.id, res.chgp, res.eid, res.sequence, res.fetchTime, false);
            return FutureOptional.empty();
        }
    }

    public long countByCrawlExecution(String executionId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String c = jedis.hget(EIDC, executionId);
            if (c == null) {
                return 0;
            }
            return Longs.tryParse(c);
        }
    }

    public long countByCrawlHostGroup(CrawlHostGroup chg) {
        try (Jedis jedis = jedisPool.getResource()) {
            String c = jedis.hget(CHG_PREFIX + createChgPolitenessKey(chg), "c");
            if (c == null) {
                return 0;
            }
            return Longs.tryParse(c);
        }
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

    private void removeQUri(String id, String chgp, String eid, long sequence, long fetchTime, boolean deleteUri) {
        long numRemoved = removeUriScript.run(id, chgp, eid, sequence, fetchTime, deleteUri);
    }

    public void removeQUri(QueuedUri qUri, String chg, boolean deleteUri) {
        String chgp = createChgPolitenessKey(chg, qUri.getPolitenessRef().getId());
        long numRemoved = removeUriScript.run(
                qUri.getId(),
                chgp,
                qUri.getExecutionId(),
                qUri.getSequence(),
                qUri.getEarliestFetchTimeStamp().getSeconds(),
                deleteUri);
    }

    static final Semaphore nextChgLock = new Semaphore(3);

    private CrawlHostGroup getNextReadyCrawlHostGroup(Context ctx) {
        try {
            while (!nextChgLock.tryAcquire(300, TimeUnit.MILLISECONDS)) {
                if (ctx.isCancelled()) {
                    return null;
                }
            }
        } catch (InterruptedException e) {
            return null;
        }
        try {
            if (ctx.isCancelled()) {
                return null;
            }
            CrawlHostGroup chg = getNextChgScript.run(BUSY_TIMEOUT);
            if (chg != null) {
                chg = chg.toBuilder().setBusy(true).build();
            }
            return chg;
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            return null;
        } finally {
            nextChgLock.release();
        }
    }

    public void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup, long nextFetchDelayMs) {
        LOG.debug("Releasing CrawlHostGroup: " + crawlHostGroup.getId() + ", with queue count: " + crawlHostGroup.getQueuedUriCount());
        crawlHostGroup = crawlHostGroup.toBuilder()
                .setNextFetchTime(ProtoUtils.odtToTs(ProtoUtils.getNowOdt().plus(nextFetchDelayMs, MILLIS)))
                .build();
        releaseCrawlHostGroup(crawlHostGroup);
    }

    public void releaseCrawlHostGroup(CrawlHostGroup crawlHostGroup) {
        releaseChgScript.run(crawlHostGroup);
    }
}
