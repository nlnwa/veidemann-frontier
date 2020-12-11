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
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
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
                    case FETCHING:
                    case SLEEPING:
                    case CREATED:
                        throw new IllegalArgumentException("Only the final states are allowed to be updated");
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
                if (change.hasDeleteCurrentUri()) {
                    rMap.with("currentUriId", doc.g("currentUriId").default_(r.array()).setDifference(r.array(change.getDeleteCurrentUri().getId())));
                }
                return doc.merge(rMap)
                        .merge(d -> r.branch(
                                // If the original document had one of the ended states, then keep the
                                // original endTime if it exists, otherwise use the one from the change request
                                doc.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", doc.g("state")).with("endTime",
                                        r.branch(doc.hasFields("endTime"), doc.g("endTime"), d.g("endTime").default_((Object) null))),

                                // If the change request contained an end state, use it
                                d.g("state").match("FINISHED|ABORTED_TIMEOUT|ABORTED_SIZE|ABORTED_MANUAL|FAILED|DIED"),
                                r.hashMap("state", d.g("state")),

                                // Set the state to fetching if currentUriId contains at least one value, otherwise set state to sleeping.
                                d.g("currentUriId").default_(r.array()).count().gt(0),
                                r.hashMap("state", "FETCHING"),
                                r.hashMap("state", "SLEEPING")))

                        // Set start time if not set and state is fetching
                        .merge(d -> r.branch(doc.hasFields("startTime").not().and(d.g("state").match("FETCHING")),
                                r.hashMap("startTime", r.now()),
                                r.hashMap()));
            };


            // Update the CrawlExecutionStatus
            Update qry = r.table(Tables.EXECUTIONS.name)
                    .get(change.getId())
                    .update(updateFunc);


            // Return both the new and the old values
            qry = qry.optArg("return_changes", "always");
            RethinkDbConnection conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
            Map<String, Object> response = conn.exec("db-updateCrawlExecutionStatus", qry);
            List<Map<String, Map>> changes = (List<Map<String, Map>>) response.get("changes");

            // Check if this update was setting the end time
            boolean wasNotEnded = changes.get(0).get("old_val") == null || changes.get(0).get("old_val").get("endTime") == null;
            CrawlExecutionStatus newDoc = ProtoUtils.rethinkToProto(changes.get(0).get("new_val"), CrawlExecutionStatus.class);
            if (wasNotEnded && newDoc.hasEndTime()) {
                frontier.updateJobExecution(newDoc.getJobExecutionId());
            }

            // Remove queued uri from queue if change request asks for deletion
            if (change.hasDeleteCurrentUri()) {
                frontier.getCrawlQueueManager().removeQUri(change.getDeleteCurrentUri(), change.getDeleteCurrentUri().getCrawlHostGroupId(), true);
            }

            status = newDoc.toBuilder();
            change = null;
            dirty = false;
        }
        return this;
    }

    public String getId() {
        return status.getId();
    }

    public ConfigRef getJobId() {
        return ConfigRef.newBuilder().setKind(Kind.crawlJob).setId(status.getJobId()).build();
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

    public StatusWrapper incrementDocumentsCrawled() {
        getChange().setAddDocumentsCrawled(1);
        return this;
    }

    public long getBytesCrawled() {
        return getCrawlExecutionStatus().getBytesCrawled();
    }

    public StatusWrapper incrementBytesCrawled(long val) {
        getChange().setAddBytesCrawled(val);
        return this;
    }

    public long getUrisCrawled() {
        return getCrawlExecutionStatus().getUrisCrawled();
    }

    public StatusWrapper incrementUrisCrawled(long val) {
        getChange().setAddUrisCrawled(val);
        return this;
    }

    public long getDocumentsFailed() {
        return getCrawlExecutionStatus().getDocumentsFailed();
    }

    public StatusWrapper incrementDocumentsFailed() {
        getChange().setAddDocumentsFailed(1);
        return this;
    }

    public long getDocumentsOutOfScope() {
        return getCrawlExecutionStatus().getDocumentsOutOfScope();
    }

    public StatusWrapper incrementDocumentsOutOfScope() {
        getChange().setAddDocumentsOutOfScope(1);
        return this;
    }

    public long getDocumentsRetried() {
        return getCrawlExecutionStatus().getDocumentsRetried();
    }

    public StatusWrapper incrementDocumentsRetried() {
        getChange().setAddDocumentsRetried(1);
        return this;
    }

    public long getDocumentsDenied() {
        return getCrawlExecutionStatus().getDocumentsDenied();
    }

    public StatusWrapper incrementDocumentsDenied(long val) {
        getChange().setAddDocumentsDenied(val);
        return this;
    }

    public CrawlExecutionStatus getCrawlExecutionStatus() {
        if (dirty) {
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
        getChange().setDeleteCurrentUri(uri.getQueuedUri());
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
