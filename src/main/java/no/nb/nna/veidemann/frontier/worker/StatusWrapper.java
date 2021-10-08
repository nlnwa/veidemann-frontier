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

import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.gen.ast.Update;
import com.rethinkdb.model.MapObject;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class StatusWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(StatusWrapper.class);

    private CrawlExecutionStatus.Builder status;
    private CrawlExecutionStatusChange.Builder change;
    private boolean dirty;
    private final Frontier frontier;
    private ConfigObject jobConfig;
    private ConfigObject crawlConfig;

    static final RethinkDB r = RethinkDB.r;

    private StatusWrapper(Frontier frontier, CrawlExecutionStatus status) {
        this.frontier = frontier;
        this.status = status.toBuilder();
    }

    private StatusWrapper(Frontier frontier, CrawlExecutionStatus.Builder status) {
        this.frontier = frontier;
        this.status = status;
    }

    public static StatusWrapper getStatusWrapper(Frontier frontier, String executionId) throws DbException {
        return new StatusWrapper(frontier, DbService.getInstance().getExecutionsAdapter().getCrawlExecutionStatus(executionId));
    }

    public static StatusWrapper getStatusWrapper(Frontier frontier, CrawlExecutionStatus status) {
        return new StatusWrapper(frontier, status);
    }

    public static StatusWrapper getStatusWrapper(Frontier frontier, CrawlExecutionStatus.Builder status) {
        return new StatusWrapper(frontier, status);
    }

    public synchronized StatusWrapper saveStatus() throws DbException {
        if (change != null) {
            change.setId(status.getId());

            ReqlFunction1 updateFunc = (doc) -> {
                MapObject rMap = r.hashMap("lastChangeTime", r.now());

                switch (change.getState()) {
                    case UNDEFINED:
                        break;
                    case CREATED:
                        throw new IllegalArgumentException("Not allowed to set state back to CREATED");
                    default:
                        rMap.with("state", change.getState().name());
                }
                if (change.getAddBytesCrawled() != 0) {
                    rMap.with("bytesCrawled", doc.g("bytesCrawled").add(change.getAddBytesCrawled()).default_(change.getAddBytesCrawled()));
                }
                if (change.getAddDocumentsCrawled() != 0) {
                    rMap.with("documentsCrawled", doc.g("documentsCrawled").add(change.getAddDocumentsCrawled()).default_(change.getAddDocumentsCrawled()));
                }
                if (change.getAddDocumentsDenied() != 0) {
                    rMap.with("documentsDenied", doc.g("documentsDenied").add(change.getAddDocumentsDenied()).default_(change.getAddDocumentsDenied()));
                }
                if (change.getAddDocumentsFailed() != 0) {
                    rMap.with("documentsFailed", doc.g("documentsFailed").add(change.getAddDocumentsFailed()).default_(change.getAddDocumentsFailed()));
                }
                if (change.getAddDocumentsOutOfScope() != 0) {
                    rMap.with("documentsOutOfScope", doc.g("documentsOutOfScope").add(change.getAddDocumentsOutOfScope()).default_(change.getAddDocumentsOutOfScope()));
                }
                if (change.getAddDocumentsRetried() != 0) {
                    rMap.with("documentsRetried", doc.g("documentsRetried").add(change.getAddDocumentsRetried()).default_(change.getAddDocumentsRetried()));
                }
                if (change.getAddUrisCrawled() != 0) {
                    rMap.with("urisCrawled", doc.g("urisCrawled").add(change.getAddUrisCrawled()).default_(change.getAddUrisCrawled()));
                }
                if (change.hasEndTime()) {
                    rMap.with("endTime", ProtoUtils.tsToOdt(change.getEndTime()));
                }
                if (change.hasError()) {
                    rMap.with("error", ProtoUtils.protoToRethink(change.getError()));
                }
                if (change.hasAddCurrentUri()) {
                    rMap.with("currentUriId", doc.g("currentUriId").default_(r.array()).setUnion(r.array(change.getAddCurrentUri().getId())));
                }

                // Remove queued uri from queue if change request asks for deletion
                if (change.hasDeleteCurrentUri()) {
                    boolean deleted = frontier.getCrawlQueueManager()
                            .removeQUri(change.getDeleteCurrentUri());

                    if (deleted) {
                        rMap.with("currentUriId", doc.g("currentUriId")
                                .default_(r.array()).setDifference(r.array(change.getDeleteCurrentUri().getId())));
                    }
                }

                return doc.merge(rMap)
                        .merge(d -> r.branch(
                                // If the original document had one of the ended states, then keep the
                                // original endTime if it exists, otherwise use the one from the change request
                                doc.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", doc.g("state")).with("endTime",
                                        r.branch(doc.hasFields("endTime"), doc.g("endTime"), d.g("endTime").default_((Object) null))),

                                // If the change request contained an end state, use it and add set start time to current time if missing.
                                d.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", d.g("state")).with("startTime",
                                        r.branch(doc.hasFields("startTime"), doc.g("startTime"), d.g("startTime").default_(r.now()))),

                                // If the change request contained FETCHING or SLEEPING state and the original document had CREATED, FETCHING or SLEEPING state,
                                // then update with changed state
                                d.g("state").match("FETCHING|SLEEPING").and(doc.g("state").match("CREATED|FETCHING|SLEEPING")),
                                r.hashMap("state", d.g("state")),
                                r.hashMap("state", doc.g("state"))))

                        // Set start time if not set and state is fetching
                        .merge(d -> r.branch(doc.hasFields("startTime").not().and(d.g("state").match("FETCHING")),
                                r.hashMap("startTime", r.now()),
                                r.hashMap()));
            };


            // Update the CrawlExecutionStatus
            Update qry = r.table(Tables.EXECUTIONS.name)
                    .get(change.getId())
                    .update(updateFunc).optArg("durability", "soft");


            // Return both the new and the old values
            qry = qry.optArg("return_changes", "always");
            RethinkDbConnection conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
            Map<String, Object> response = conn.exec("db-updateCrawlExecutionStatus", qry);
            List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

            // Check if this update was setting the end time
            boolean wasNotEnded = changes.get(0).get("old_val") == null || changes.get(0).get("old_val").get("endTime") == null;
            CrawlExecutionStatus newDoc = ProtoUtils.rethinkToProto(changes.get(0).get("new_val"), CrawlExecutionStatus.class);

            Boolean hasRunningExecutions = frontier.getCrawlQueueManager().updateJobExecutionStatus(newDoc.getJobExecutionId(), status.getState(), newDoc.getState(), change);
            if (!hasRunningExecutions && wasNotEnded && newDoc.hasEndTime()) {
                updateJobExecution(conn, newDoc.getJobExecutionId());
            }

            status = newDoc.toBuilder();
            change = null;
            dirty = false;
        }
        return this;
    }

    private void updateJobExecution(RethinkDbConnection conn, String jobExecutionId) throws DbException {
        JobExecutionStatus tjes = frontier.getCrawlQueueManager().getTempJobExecutionStatus(jobExecutionId);
        if (tjes == null) {
            return;
        }

        LOG.debug("JobExecution '{}' finished, saving stats", jobExecutionId);

        // Fetch the JobExecutionStatus object this CrawlExecution is part of
        JobExecutionStatus jes = conn.executeGet("db-getJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name).get(jobExecutionId),
                JobExecutionStatus.class);
        if (jes == null) {
            throw new IllegalStateException("Can't find JobExecution: " + jobExecutionId);
        }

        // Set JobExecution's status to FINISHED if it wasn't already aborted
        JobExecutionStatus.State state;
        switch (jes.getState()) {
            case FAILED:
            case ABORTED_MANUAL:
                state = jes.getState();
                break;
            default:
                if (jes.getDesiredState() != null && jes.getDesiredState() != JobExecutionStatus.State.UNDEFINED) {
                    state = jes.getDesiredState();
                } else {
                    state = JobExecutionStatus.State.FINISHED;
                }
                break;
        }

        // Update aggregated statistics
        JobExecutionStatus.Builder jesBuilder = jes.toBuilder()
                .setState(state)
                .setEndTime(ProtoUtils.getNowTs());
        jesBuilder.mergeFrom(tjes);

        conn.exec("db-saveJobExecutionStatus",
                r.table(Tables.JOB_EXECUTIONS.name).get(jesBuilder.getId()).update(ProtoUtils.protoToRethink(jesBuilder)));
        frontier.getCrawlQueueManager().removeRedisJobExecution(jobExecutionId);
    }

    public String getId() {
        return status.getId();
    }

    public ConfigObject getCrawlJobConfig() throws DbQueryException {
        if (jobConfig == null) {
            jobConfig = frontier.getConfig(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId(status.getJobId()).build());
        }
        return jobConfig;
    }

    public ConfigObject getCrawlConfig() throws DbQueryException {
        if (crawlConfig == null) {
            crawlConfig = frontier.getConfig(getCrawlJobConfig().getCrawlJob().getCrawlConfigRef());
        }
        return crawlConfig;
    }

    public String getJobExecutionId() {
        return status.getJobExecutionId();
    }

    public Timestamp getStartTime() {
        return getCrawlExecutionStatus().getStartTime();
    }

    public Timestamp getCreatedTime() {
        return getCrawlExecutionStatus().getCreatedTime();
    }

    public Timestamp getEndTime() {
        return getCrawlExecutionStatus().getEndTime();
    }

    public boolean isEnded() {
        return getCrawlExecutionStatus().hasEndTime();
    }

    public CrawlExecutionStatus.State getState() {
        return getCrawlExecutionStatus().getState();
    }

    public CrawlExecutionStatus.State getDesiredState() {
        return status.getDesiredState();
    }

    public StatusWrapper setState(CrawlExecutionStatus.State state) {
        dirty = true;
        getChange().setState(state);
        return this;
    }

    public StatusWrapper setEndState(CrawlExecutionStatus.State state) {
        LOG.debug("Reached end of crawl '{}' with state: {}", getId(), state);
        dirty = true;
        getChange().setState(state).setEndTime(ProtoUtils.getNowTs());
        return this;
    }

    public long getDocumentsCrawled() {
        return getCrawlExecutionStatus().getDocumentsCrawled();
    }

    public synchronized StatusWrapper incrementDocumentsCrawled() {
        getChange().setAddDocumentsCrawled(getChange().getAddDocumentsCrawled() + 1);
        return this;
    }

    public long getBytesCrawled() {
        return getCrawlExecutionStatus().getBytesCrawled();
    }

    public synchronized StatusWrapper incrementBytesCrawled(long val) {
        getChange().setAddBytesCrawled(getChange().getAddBytesCrawled() + val);
        return this;
    }

    public long getUrisCrawled() {
        return getCrawlExecutionStatus().getUrisCrawled();
    }

    public synchronized StatusWrapper incrementUrisCrawled(long val) {
        getChange().setAddUrisCrawled(getChange().getAddDocumentsCrawled() + val);
        return this;
    }

    public long getDocumentsFailed() {
        return getCrawlExecutionStatus().getDocumentsFailed();
    }

    public synchronized StatusWrapper incrementDocumentsFailed() {
        getChange().setAddDocumentsFailed(getChange().getAddDocumentsFailed() + 1);
        return this;
    }

    public long getDocumentsOutOfScope() {
        return getCrawlExecutionStatus().getDocumentsOutOfScope();
    }

    public synchronized StatusWrapper incrementDocumentsOutOfScope() {
        getChange().setAddDocumentsOutOfScope(getChange().getAddDocumentsOutOfScope() + 1);
        return this;
    }

    public long getDocumentsRetried() {
        return getCrawlExecutionStatus().getDocumentsRetried();
    }

    public synchronized StatusWrapper incrementDocumentsRetried() {
        getChange().setAddDocumentsRetried(getChange().getAddDocumentsRetried() + 1);
        return this;
    }

    public long getDocumentsDenied() {
        return getCrawlExecutionStatus().getDocumentsDenied();
    }

    public synchronized StatusWrapper incrementDocumentsDenied(long val) {
        getChange().setAddDocumentsDenied(getChange().getAddDocumentsDenied() + val);
        return this;
    }

    public CrawlExecutionStatus getCrawlExecutionStatus() {
        if (dirty) {
            new RuntimeException("CES").printStackTrace();
            throw new IllegalStateException("CES is dirty " + change);
        }
        return status.build();
    }

    public StatusWrapper addCurrentUri(QueuedUri uri) {
        getChange().setAddCurrentUri(uri);
        return this;
    }

    public StatusWrapper addCurrentUri(QueuedUriWrapper uri) {
        getChange().setAddCurrentUri(uri.getQueuedUri());
        return this;
    }

    public StatusWrapper removeCurrentUri(QueuedUri uri) {
        getChange().setDeleteCurrentUri(uri);
        return this;
    }

    public StatusWrapper removeCurrentUri(QueuedUriWrapper uri) {
        getChange().setDeleteCurrentUri(uri.getQueuedUriForRemoval());
        return this;
    }

    public StatusWrapper setError(Error error) {
        getChange().setError(error);
        return this;
    }

    public StatusWrapper setError(int code, String message) {
        getChange().setError(Error.newBuilder().setCode(code).setMsg(message).build());
        return this;
    }

    private CrawlExecutionStatusChange.Builder getChange() {
        if (change == null) {
            change = CrawlExecutionStatusChange.newBuilder();
        }
        return change;
    }
}
