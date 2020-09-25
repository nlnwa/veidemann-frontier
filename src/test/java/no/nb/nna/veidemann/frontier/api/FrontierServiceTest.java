package no.nb.nna.veidemann.frontier.api;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.CrawlableUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbInitializer;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import no.nb.nna.veidemann.frontier.db.CrawlQueueManager;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.frontier.worker.QueuedUriWrapper;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class FrontierServiceTest {
    private final String uniqueServerName = "in-process server for " + getClass();

    @After
    public void afterEachTest() {
        validateMockitoUsage();
    }

    @Test
    public void crawlSeed() {
    }

    @Test
    public void getNextPage() throws InterruptedException, ExecutionException, IOException, TimeoutException, DbException {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        DbInitializer dbInitMock = mock(RethinkDbInitializer.class);
        ExecutionsAdapter executionsAdapterMock = mock(ExecutionsAdapter.class);
        when(dbProviderMock.getExecutionsAdapter()).thenReturn(executionsAdapterMock);
        when(dbProviderMock.getDbInitializer()).thenReturn(dbInitMock);
        when(executionsAdapterMock.getCrawlExecutionStatus(anyString())).thenReturn(CrawlExecutionStatus.newBuilder().build());
        try (DbService db = DbService.configure(dbProviderMock)) {
            Frontier frontierMock = mock(Frontier.class);
            CrawlExecution crawlExecutionMock = mock(CrawlExecution.class);
            CrawlQueueManager crawlQueueManagerMock = mock(CrawlQueueManager.class);

            InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName);
            InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(uniqueServerName);
            FrontierApiServer inProcessServer = new FrontierApiServer(inProcessServerBuilder, frontierMock).start();

            FrontierClientMock client = new FrontierClientMock(inProcessChannelBuilder);

            PageHarvestSpec.Builder harvestSpec = PageHarvestSpec.newBuilder();
            harvestSpec.getCrawlConfigBuilder().setId("foo");

            QueuedUri uri1 = QueuedUri.getDefaultInstance();
            QueuedUriWrapper queuedUriWrapperMock1 = mock(QueuedUriWrapper.class);
            when(queuedUriWrapperMock1.getQueuedUri()).thenReturn(uri1);
            PageHarvestSpec harvestSpec1 = harvestSpec.setQueuedUri(uri1).build();

            when(frontierMock.getCrawlQueueManager()).thenReturn(crawlQueueManagerMock);
            when(frontierMock.getConfig(any())).thenReturn(ConfigObject.newBuilder().build());
            when(crawlQueueManagerMock.getNextToFetch(any())).thenReturn(new CrawlableUri(null, uri1));
            when(crawlQueueManagerMock.createCrawlExecution(any(), any())).thenReturn(crawlExecutionMock);

            when(crawlExecutionMock.getUri()).thenReturn(queuedUriWrapperMock1);
            when(crawlExecutionMock.preFetch()).thenReturn(harvestSpec1);
            doNothing()
                    .doThrow(new RuntimeException("Simulated exception in postFetchSuccess"))
                    .when(crawlExecutionMock).postFetchSuccess(any());
            doNothing()
                    .doNothing()
                    .doThrow(new RuntimeException("Simulated exception in postFetchFailure"))
                    .when(crawlExecutionMock).postFetchFailure((Error) any());
            doNothing()
                    .doThrow(new RuntimeException("Simulated exception in queueOutlink"))
                    .when(crawlExecutionMock).queueOutlink(any());

            client.requestNext(FrontierClientMock.SUCCESS);
            client.requestNext(FrontierClientMock.SUCCESS);
            client.requestNext(FrontierClientMock.SUCCESS_WITH_OUTLINKS);
            client.requestNext(FrontierClientMock.SUCCESS_WITH_OUTLINKS);
            client.requestNext(FrontierClientMock.EXCEPTION);
            client.requestNext(FrontierClientMock.ERROR);
            client.requestNext(FrontierClientMock.ERROR);

            client.close();
            inProcessServer.shutdown();
            inProcessServer.blockUntilShutdown();

            verify(frontierMock, times(14)).getCrawlQueueManager();
            verify(frontierMock, atLeastOnce()).checkHealth();
            verify(crawlExecutionMock, times(14)).getUri();
            verify(crawlExecutionMock, times(7)).preFetch();
            verify(crawlExecutionMock, times(7)).postFetchFinally();
            verify(crawlExecutionMock, times(4)).postFetchSuccess(any());
            verify(crawlExecutionMock, times(2)).queueOutlink(any());
            verify(crawlExecutionMock, times(2))
                    .postFetchFailure(Error.newBuilder().setCode(1).setMsg("Error").build());
            verify(crawlExecutionMock, times(1))
                    .postFetchFailure(ExtraStatusCodes.RUNTIME_EXCEPTION
                            .toFetchError("java.lang.RuntimeException: Simulated render exception"));

            verifyNoMoreInteractions(frontierMock, crawlExecutionMock);
        }
    }
}
