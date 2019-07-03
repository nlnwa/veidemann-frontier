package no.nb.nna.veidemann.frontier.api;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.PageHarvestSpec;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.ExtraStatusCodes;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FrontierServiceTest {
    private final String uniqueServerName = "in-process server for " + getClass();

    @After
    public void afterEachTest() {
        validateMockitoUsage();
    }

    @Test
    @Ignore
    public void crawlSeed() {
    }

    @Test
    public void getNextPage() throws InterruptedException, ExecutionException, IOException, TimeoutException, DbException {
        Frontier frontierMock = mock(Frontier.class);
        CrawlExecution crawlExecutionMock = mock(CrawlExecution.class);

        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName);
        InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(uniqueServerName);
        FrontierApiServer inProcessServer = new FrontierApiServer(inProcessServerBuilder, frontierMock).start();

        FrontierClientMock client = new FrontierClientMock(inProcessChannelBuilder);

        PageHarvestSpec.Builder harvestSpec = PageHarvestSpec.newBuilder();
        harvestSpec.getCrawlConfigBuilder().setId("foo");

        QueuedUri uri1 = QueuedUri.newBuilder().setUri("http://example1.com").build();
        QueuedUri uri2 = QueuedUri.newBuilder().setUri("http://example2.com").build();
        PageHarvestSpec harvestSpec1 = harvestSpec.setQueuedUri(uri1).build();
        PageHarvestSpec harvestSpec2 = harvestSpec.setQueuedUri(uri2).build();

        when(frontierMock.getNextPageToFetch()).thenReturn(crawlExecutionMock);

        when(crawlExecutionMock.preFetch()).thenReturn(harvestSpec1, harvestSpec2);
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
        inProcessServer.close();
        inProcessServer.blockUntilShutdown();

        verify(frontierMock, times(7)).getNextPageToFetch();
        verify(frontierMock, times(7)).setCurrentClientCount(anyInt());
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