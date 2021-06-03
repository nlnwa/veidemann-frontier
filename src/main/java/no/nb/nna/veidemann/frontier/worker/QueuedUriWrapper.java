/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.frontier.worker;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUriOrBuilder;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckRequest;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse.Evaluation;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.db.ProtoUtils.rethinkToProto;

/**
 *
 */
public class QueuedUriWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(QueuedUriWrapper.class);
    static final RethinkDB r = RethinkDB.r;

    private QueuedUri.Builder wrapped;

    private String host;
    private int port;
    private final String collectionName;
    private String includedCheckUri;
    private ScopeCheckResponse scopeCheckResponse;
    private CrawlHostGroup.Builder crawlHostGroup;
    private Timestamp oldEarliestFetchTimestamp;

    final Frontier frontier;

    private final static String ALL_CHGS_CACHE_KEY = "all_chg";
    static final LoadingCache<String, Map<String, ConfigObject>> chgConfigCache;

    static {
        chgConfigCache = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Map<String, ConfigObject>>() {
                            public Map<String, ConfigObject> load(String key) throws DbException {
                                switch (key) {
                                    case ALL_CHGS_CACHE_KEY:
                                        try (ChangeFeed<ConfigObject> cursor = DbService.getInstance().getConfigAdapter()
                                                .listConfigObjects(ListRequest.newBuilder()
                                                        .setKind(Kind.crawlHostGroupConfig).build())) {
                                            return cursor.stream().collect(
                                                    Collectors.toUnmodifiableMap(ConfigObject::getId, Function.identity()));
                                        }
                                    default:
                                        throw new IllegalArgumentException("Unknown chg cache key: " + key);
                                }
                            }
                        });
    }

    private QueuedUriWrapper(
            Frontier frontier, QueuedUriOrBuilder uri, String collectionName) {

        this.frontier = frontier;
        this.collectionName = collectionName;
        if (uri instanceof QueuedUri.Builder) {
            wrapped = (QueuedUri.Builder) uri;
        } else {
            wrapped = ((QueuedUri) uri).toBuilder();
        }
    }

    public void initScopeCheck(Collection<Annotation> scriptParameters,
                               ConfigRef scopeScriptRef) throws DbQueryException {
        wrapped.addAllAnnotation(scriptParameters);
        ConfigObject script = frontier.getConfig(scopeScriptRef);

        ScopeCheckRequest scopeCheckRequest = ScopeCheckRequest.newBuilder()
                .setScopeScriptName(script.getMeta().getName())
                .setScopeScript(script.getBrowserScript().getScript())
                .setDebug(false)
                .setQueuedUri(wrapped.build())
                .build();
        LOG.trace("Scope check request: {}", scopeCheckRequest);
        scopeCheckResponse = frontier.getScopeServiceClient().scopeCheck(scopeCheckRequest);
        LOG.trace("Scope check response: {}", scopeCheckResponse);
        includedCheckUri = scopeCheckResponse.getIncludeCheckUri().getHref();
        host = scopeCheckResponse.getIncludeCheckUri().getHost();
        port = scopeCheckResponse.getIncludeCheckUri().getPort();
    }

    public static QueuedUriWrapper getQueuedUriWrapperNoScopeCheck(Frontier frontier, QueuedUri qUri, String collectionName) {
        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(qUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(qUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(qUri.getPolitenessRef(), "Empty PolitenessRef");

        return new QueuedUriWrapper(frontier, qUri, collectionName);
    }

    public static QueuedUriWrapper getQueuedUriWrapperWithScopeCheck(
            Frontier frontier, QueuedUriOrBuilder qUri, String collectionName,
            Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef) throws URISyntaxException, DbException {

        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(qUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(qUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(qUri.getPolitenessRef(), "Empty PolitenessRef");

        QueuedUriWrapper wrapper = new QueuedUriWrapper(frontier, qUri, collectionName);
        wrapper.initScopeCheck(scriptParameters, scopeScriptRef);
        return wrapper;
    }

    public static QueuedUriWrapper getOutlinkQueuedUriWrapper(
            Frontier frontier, QueuedUriWrapper parentUri, QueuedUri.Builder qUri, String collectionName,
            Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef) throws URISyntaxException, DbException {

        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(parentUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(parentUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(parentUri.getPolitenessRef(), "Empty PolitenessRef");
        requireNonEmpty(parentUri.getSeedUri(), "Empty SeedUri");

        qUri.setSeedUri(parentUri.getSeedUri())
                .setJobExecutionId(parentUri.getJobExecutionId())
                .setExecutionId(parentUri.getExecutionId())
                .setPolitenessRef(parentUri.getPolitenessRef())
                .setSequence(parentUri.getSequence() + 1);

        QueuedUriWrapper wrapper = getQueuedUriWrapperWithScopeCheck(frontier, qUri, parentUri.collectionName, scriptParameters, scopeScriptRef);
        wrapper.wrapped.setUnresolved(true);

        return wrapper;
    }

    public static QueuedUriWrapper createSeedQueuedUri(
            Frontier frontier, String uri, String jobExecutionId, String executionId, ConfigRef politenessId,
            String collectionName, Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef)
            throws URISyntaxException, DbException {

        return getQueuedUriWrapperWithScopeCheck(frontier, QueuedUri.newBuilder()
                        .setUri(uri)
                        .setSeedUri(uri)
                        .setJobExecutionId(jobExecutionId)
                        .setExecutionId(executionId)
                        .setPolitenessRef(politenessId)
                        .setUnresolved(true)
                        .setSequence(1L),
                collectionName,
                scriptParameters,
                scopeScriptRef
        );
    }

    public String getIncludedCheckUri() {
        return includedCheckUri;
    }

    public boolean shouldInclude() {
        return scopeCheckResponse.getEvaluation() == Evaluation.INCLUDE;
    }

    public int getExcludedReasonStatusCode() {
        if (shouldInclude()) {
            throw new IllegalStateException("Exclude reason called on uri which was eligible for inclusion");
        }
        return scopeCheckResponse.getExcludeReason();
    }

    public boolean hasExcludedError() {
        return scopeCheckResponse != null && scopeCheckResponse.hasError();
    }

    public Error getExcludedError() {
        if (shouldInclude()) {
            throw new IllegalStateException("Exclude reason called on uri which was eligible for inclusion");
        }
        return scopeCheckResponse.getError();
    }

    public QueuedUriWrapper save() throws DbQueryException, DbConnectionException {
        Map rMap = ProtoUtils.protoToRethink(wrapped);

        Map<String, Object> response = frontier.conn.exec("db-saveQueuedUri",
                r.table(Tables.URI_QUEUE.name).get(wrapped.getId())
                        .update(rMap)
                        .optArg("durability", "soft")
                        .optArg("return_changes", "always"));
        List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

        Map newDoc = changes.get(0).get("new_val");
        wrapped = rethinkToProto(newDoc, QueuedUri.class).toBuilder();

        if (oldEarliestFetchTimestamp != null && oldEarliestFetchTimestamp.getSeconds() != wrapped.getEarliestFetchTimeStamp().getSeconds()) {
            frontier.getCrawlQueueManager().updateQueuedUri(this, oldEarliestFetchTimestamp);
            oldEarliestFetchTimestamp = null;
        }
        return this;
    }

    public boolean addUriToQueue(StatusWrapper status) throws DbException {
        if (frontier.getCrawlQueueManager().uriNotIncludedInQueue(this)) {
            return forceAddUriToQueue(status);
        }
        LOG.debug("Found already included URI: {}, skipping.", getUri());
        setError(ExtraStatusCodes.ALREADY_SEEN.toFetchError("Uri was already harvested"));
        return false;
    }

    /**
     * Adds Uri to queue without checking if it is already included. Scope check is applied though,
     *
     * @return
     * @throws DbException
     */
    public boolean forceAddUriToQueue(StatusWrapper status) throws DbException {
        if (!shouldInclude()) {
            return false;
        }

        requireNonEmpty(wrapped.getUri(), "Empty URI string");
        requireNonEmpty(wrapped.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(wrapped.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(wrapped.getPolitenessRef(), "Empty PolitenessRef");
        if (wrapped.getSequence() <= 0) {
            String msg = "Uri is missing Sequence. Uri: " + wrapped.getUri();
            IllegalStateException ex = new IllegalStateException(msg);
            LOG.error(msg, ex);
            throw ex;
        }
        if (wrapped.getPriorityWeight() <= 0d) {
            String msg = "Priority weight must be greater than zero. Uri: " + wrapped.getUri();
            IllegalStateException ex = new IllegalStateException(msg);
            LOG.error(msg, ex);
            throw ex;
        }

        if (wrapped.getCrawlHostGroupId().isEmpty()) {

            ConfigObject politeness = frontier.getConfig(wrapped.getPolitenessRef());
            wrapped.setCrawlHostGroupId(CrawlHostGroupCalculator.calculateCrawlHostGroupId(getHost(), null,
                    Collections.emptyList(), politeness));
        }
        requireNonEmpty(wrapped.getCrawlHostGroupId(), "Empty CrawlHostGroupId");

        // Annotations are dynamic and should not be stored in DB
        wrapped.clearAnnotation();

        QueuedUri q = wrapped.build();
        q = frontier.getCrawlQueueManager().addToCrawlHostGroup(q);
        wrapped = q.toBuilder();

        oldEarliestFetchTimestamp = null;

        return true;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUri() {
        return wrapped.getUri();
    }

    public String getSeedUri() {
        return wrapped.getSeedUri();
    }

    public QueuedUriWrapper setSeedUri(String seedUri) {
        wrapped.setSeedUri(seedUri);
        return this;
    }

    public List<Annotation> getAnnotationList() {
        return wrapped.getAnnotationList();
    }

    public String getIp() {
        return wrapped.getIp();
    }

    QueuedUriWrapper setIp(String value) {
        wrapped.setIp(value);
        return this;
    }

    public String getExecutionId() {
        return wrapped.getExecutionId();
    }

    QueuedUriWrapper setExecutionId(String id) {
        wrapped.setExecutionId(id);
        return this;
    }

    public String getJobExecutionId() {
        return wrapped.getJobExecutionId();
    }

    QueuedUriWrapper setJobExecutionId(String id) {
        wrapped.setJobExecutionId(id);
        return this;
    }

    public int getRetries() {
        return wrapped.getRetries();
    }

    QueuedUriWrapper setRetries(int value) {
        wrapped.setRetries(value);
        return this;
    }

    public Timestamp getFetchStartTimeStamp() {
        return wrapped.getFetchStartTimeStamp();
    }

    QueuedUriWrapper setFetchStartTimeStamp(Timestamp value) {
        wrapped.setFetchStartTimeStamp(value);
        if (crawlHostGroup != null) {
            crawlHostGroup.setFetchStartTimeStamp(value);
        }
        return this;
    }

    String getDiscoveryPath() {
        return wrapped.getDiscoveryPath();
    }

    String getReferrer() {
        return wrapped.getReferrer();
    }

    boolean hasError() {
        return wrapped.hasError();
    }

    Error getError() {
        return wrapped.getError();
    }

    public QueuedUriWrapper setError(Error value) {
        wrapped.setError(value);
        return this;
    }

    QueuedUriWrapper clearError() {
        wrapped.clearError();
        return this;
    }

    public String getCrawlHostGroupId() {
        return wrapped.getCrawlHostGroupId();
    }

    QueuedUriWrapper setCrawlHostGroupId(String value) {
        wrapped.setCrawlHostGroupId(value);
        return this;
    }

    String generateSessionToken() {
        String uuid = UUID.randomUUID().toString();
        crawlHostGroup.setSessionToken(uuid);
        return uuid;
    }

    public CrawlHostGroup getCrawlHostGroup() throws DbQueryException {
        if (crawlHostGroup == null) {
            try {
                CrawlHostGroup chg = frontier.getCrawlQueueManager().getCrawlHostGroup(wrapped.getCrawlHostGroupId());
                Map<String, ConfigObject> groupConfigs = chgConfigCache.get(ALL_CHGS_CACHE_KEY);
                ConfigObject chgConfig = groupConfigs.getOrDefault(wrapped.getCrawlHostGroupId(),
                        frontier.getConfig(ConfigRef.newBuilder().setKind(Kind.crawlHostGroupConfig).setId("chg-default").build()));

                crawlHostGroup = CrawlHostGroup.newBuilder()
                        .setId(wrapped.getCrawlHostGroupId())
                        .setCurrentUriId(wrapped.getId())
                        .setMinTimeBetweenPageLoadMs(chgConfig.getCrawlHostGroupConfig().getMinTimeBetweenPageLoadMs())
                        .setMaxTimeBetweenPageLoadMs(chgConfig.getCrawlHostGroupConfig().getMaxTimeBetweenPageLoadMs())
                        .setDelayFactor(chgConfig.getCrawlHostGroupConfig().getDelayFactor())
                        .setMaxRetries(chgConfig.getCrawlHostGroupConfig().getMaxRetries())
                        .setRetryDelaySeconds(chgConfig.getCrawlHostGroupConfig().getRetryDelaySeconds())
                        .setFetchStartTimeStamp(wrapped.getFetchStartTimeStamp())
                        .mergeFrom(chg);
            } catch (ExecutionException e) {
                throw new DbQueryException(e);
            }
        }
        return crawlHostGroup.build();
    }

    QueuedUriWrapper setEarliestFetchDelaySeconds(int value) {
        Timestamp earliestFetch = Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(value));
        if (oldEarliestFetchTimestamp == null && wrapped.hasEarliestFetchTimeStamp()) {
            oldEarliestFetchTimestamp = wrapped.getEarliestFetchTimeStamp();
        }
        wrapped.setEarliestFetchTimeStamp(earliestFetch);
        return this;
    }

    ConfigRef getPolitenessRef() {
        return wrapped.getPolitenessRef();
    }

    QueuedUriWrapper setPolitenessRef(ConfigRef value) {
        wrapped.setPolitenessRef(value);
        return this;
    }

    public long getSequence() {
        return wrapped.getSequence();
    }

    QueuedUriWrapper setSequence(long value) {
        if (value < 0L) {
            throw new IllegalArgumentException("Negative values not allowed");
        }

        wrapped.setSequence(value);
        return this;
    }

    public double getPriorityWeight() {
        return wrapped.getPriorityWeight();
    }

    QueuedUriWrapper setPriorityWeight(double value) {
        if (value <= 0d) {
            value = 1.0d;
            LOG.debug("Priority weight should be greater than zero. Using default of 1.0 for uri: {}", wrapped.getUri());
        }

        wrapped.setPriorityWeight(value);
        return this;
    }

    QueuedUriWrapper incrementRetries() {
        wrapped.setRetries(wrapped.getRetries() + 1);
        return this;
    }

    public boolean isUnresolved() {
        return wrapped.getUnresolved();
    }

    QueuedUriWrapper setResolved(ConfigObject politeness) throws DbException {
        if (wrapped.getUnresolved() == false) {
            throw new RuntimeException("Uri '" + getUri() + "'was already resolved to " + getIp());
        }

        if (wrapped.getIp().isEmpty()) {
            String msg = "Can't set Uri to resolved when ip is missing. Uri: " + wrapped.getUri();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        try {
            Map<String, ConfigObject> groupConfigs = chgConfigCache.get(ALL_CHGS_CACHE_KEY);
            String crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroupId(getHost(), wrapped.getIp(), groupConfigs.values(), politeness);
            wrapped.setCrawlHostGroupId(crawlHostGroupId);
            wrapped.setUnresolved(false);
            return this;
        } catch (ExecutionException e) {
            throw new DbQueryException(e);
        }
    }

    public QueuedUri getQueuedUri() {
        return wrapped.build();
    }

    public QueuedUri getQueuedUriForRemoval() {
        if (oldEarliestFetchTimestamp != null) {
            return wrapped.clone().setEarliestFetchTimeStamp(oldEarliestFetchTimestamp).build();
        } else {
            return wrapped.build();
        }
    }

    public String getCollectionName() {
        return collectionName;
    }

    private static void requireNonEmpty(String obj, String message) {
        if (obj == null || obj.isEmpty()) {
            LOG.error(message);
            throw new NullPointerException(message);
        }
    }

    private static void requireNonEmpty(ConfigRef obj, String message) {
        if (obj == null || obj.getId().isEmpty() || obj.getKind() == Kind.undefined) {
            LOG.error(message);
            throw new NullPointerException(message);
        }
    }

    @Override
    public String toString() {
        return getUri().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueuedUriWrapper that = (QueuedUriWrapper) o;
        return getUri().equals(that.getUri());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUri());
    }
}
