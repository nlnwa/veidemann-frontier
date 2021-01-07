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
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUriOrBuilder;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckRequest;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse;
import no.nb.nna.veidemann.api.scopechecker.v1.ScopeCheckResponse.Evaluation;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class QueuedUriWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(QueuedUriWrapper.class);

    private QueuedUri.Builder wrapped;

    private final String host;
    private final int port;
    private final String collectionName;
    private final String includedCheckUri;
    private final ScopeCheckResponse scopeCheckResponse;

    final Frontier frontier;

    static final LoadingCache<ListRequest, List<ConfigObject>> chgConfigCache;

    static {
        chgConfigCache = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<ListRequest, List<ConfigObject>>() {
                            public List<ConfigObject> load(ListRequest key) throws DbException {
                                try (ChangeFeed<ConfigObject> cursor = DbService.getInstance().getConfigAdapter().listConfigObjects(key)) {
                                    return cursor.stream().collect(Collectors.toList());
                                }
                            }
                        });
    }

    private QueuedUriWrapper(
            Frontier frontier, QueuedUriOrBuilder uri, String collectionName, Collection<Annotation> scriptParameters,
            ConfigRef scopeScriptRef) throws URISyntaxException, DbException {

        this.frontier = frontier;
        this.collectionName = collectionName;
        if (uri instanceof QueuedUri.Builder) {
            wrapped = (QueuedUri.Builder) uri;
        } else {
            wrapped = ((QueuedUri) uri).toBuilder();
        }
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

    public static QueuedUriWrapper getQueuedUriWrapper(
            Frontier frontier, QueuedUri qUri, String collectionName,
            Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef) throws URISyntaxException, DbException {

        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(qUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(qUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(qUri.getPolitenessRef(), "Empty PolitenessRef");

        return new QueuedUriWrapper(frontier, qUri, collectionName, scriptParameters, scopeScriptRef);
    }

    public static QueuedUriWrapper getOutlinkQueuedUriWrapper(
            Frontier frontier, QueuedUriWrapper parentUri, QueuedUri qUri, String collectionName,
            Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef) throws URISyntaxException, DbException {

        requireNonEmpty(qUri.getUri(), "Empty URI string");
        requireNonEmpty(parentUri.getJobExecutionId(), "Empty JobExecutionId");
        requireNonEmpty(parentUri.getExecutionId(), "Empty ExecutionId");
        requireNonEmpty(parentUri.getPolitenessRef(), "Empty PolitenessRef");
        requireNonEmpty(parentUri.getSeedUri(), "Empty SeedUri");

        qUri = qUri.toBuilder()
                .setSeedUri(parentUri.getSeedUri())
                .setJobExecutionId(parentUri.getJobExecutionId())
                .setExecutionId(parentUri.getExecutionId())
                .setPolitenessRef(parentUri.getPolitenessRef())
                .build();

        QueuedUriWrapper wrapper = new QueuedUriWrapper(frontier, qUri, collectionName, scriptParameters, scopeScriptRef);
        wrapper.wrapped.setUnresolved(true);

        return wrapper;
    }

    public static QueuedUriWrapper createSeedQueuedUri(
            Frontier frontier, String uri, String jobExecutionId, String executionId, ConfigRef politenessId,
            String collectionName, Collection<Annotation> scriptParameters, ConfigRef scopeScriptRef)
            throws URISyntaxException, DbException {

        return new QueuedUriWrapper(frontier, QueuedUri.newBuilder()
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

    public Error getExcludedError() {
        if (shouldInclude()) {
            throw new IllegalStateException("Exclude reason called on uri which was eligible for inclusion");
        }
        return scopeCheckResponse.getError();
    }

    public boolean addUriToQueue(StatusWrapper status) throws DbException {
        if (frontier.getCrawlQueueManager().uriNotIncludedInQueue(this)) {
            return forceAddUriToQueue(status);
        }
        LOG.debug("Found already included URI: {}, skipping.", getUri());
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
            logOutOfScope(status);
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

        if (wrapped.getUnresolved() && wrapped.getCrawlHostGroupId().isEmpty()) {
            wrapped.setCrawlHostGroupId("TEMP_CHG_" + ApiTools.createSha1Digest(getHost()));
        }
        requireNonEmpty(wrapped.getCrawlHostGroupId(), "Empty CrawlHostGroupId");

        // Annotations are dynamic and should not be stored in DB
        wrapped.clearAnnotation();

        QueuedUri q = wrapped.build();
        q = frontier.getCrawlQueueManager().addToCrawlHostGroup(q);
        wrapped = q.toBuilder();

        return true;
    }

    public void logOutOfScope(StatusWrapper status) throws DbException {
        LOG.debug("URI '{}' is out of scope, skipping.", getUri());
        if (scopeCheckResponse.hasError()) {
            setError(scopeCheckResponse.getError());
            DbUtil.writeLog(this);
        } else {
            DbUtil.writeLog(this, scopeCheckResponse.getExcludeReason());
        }
        status.incrementDocumentsOutOfScope();
        frontier.getOutOfScopeHandlerClient().submitUri(getQueuedUri());
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

    QueuedUriWrapper setError(Error value) {
        wrapped.setError(value);
        return this;
    }

    QueuedUriWrapper clearError() {
        wrapped.clearError();
        return this;
    }

    String getCrawlHostGroupId() {
        return wrapped.getCrawlHostGroupId();
    }

    QueuedUriWrapper setCrawlHostGroupId(String value) {
        wrapped.setCrawlHostGroupId(value);
        return this;
    }

    QueuedUriWrapper setEarliestFetchDelaySeconds(int value) {
        Timestamp earliestFetch = Timestamps.add(ProtoUtils.getNowTs(), Durations.fromSeconds(value));
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
        // Internally use a sequence with base 1 to avoid the value beeing deleted from the protobuf object.
        return wrapped.getSequence() - 1L;
    }

    QueuedUriWrapper setSequence(long value) {
        if (value < 0L) {
            throw new IllegalArgumentException("Negative values not allowed");
        }

        // Internally use a sequence with base 1 to avoid the value beeing deleted from the protobuf object.
        wrapped.setSequence(value + 1L);
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
            return this;
        }

        if (wrapped.getIp().isEmpty()) {
            String msg = "Can't set Uri to resolved when ip is missing. Uri: " + wrapped.getUri();
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        String crawlHostGroupId;
        if (politeness.getPolitenessConfig().getUseHostname()) {
            // Use host name for politeness
            crawlHostGroupId = ApiTools.createSha1Digest(getHost());
        } else {
            // Use IP for politeness
            // Calculate CrawlHostGroup
            try {
                List<ConfigObject> groupConfigs = chgConfigCache.get(ListRequest.newBuilder()
                        .setKind(Kind.crawlHostGroupConfig)
                        .addAllLabelSelector(politeness.getPolitenessConfig().getCrawlHostGroupSelectorList()).build());
                crawlHostGroupId = CrawlHostGroupCalculator.calculateCrawlHostGroup(wrapped.getIp(), groupConfigs);
            } catch (ExecutionException e) {
                throw new DbQueryException(e);
            }
        }

        wrapped.setCrawlHostGroupId(crawlHostGroupId);

        wrapped.setUnresolved(false);
        return this;
    }

    public QueuedUri getQueuedUri() {
        return wrapped.build();
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
