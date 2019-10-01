package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.core.executor.BufferedReordering;
import no.ssb.dc.core.executor.FixedThreadPool;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Listeners(TestServerListener.class)
public class SiriusWorkerTest {

    @Inject
    TestServer testServer;

    Services services;

    @BeforeMethod
    public void setup() {
        services = Services.create()
                .register(ConfigurationMap.class, new ConfigurationMap(testServer.getConfiguration().asMap()))
                .register(Client.class, Client.newClientBuilder().build())
                .register(BufferedReordering.class, new BufferedReordering<>())
                .register(ContentStore.class, ProviderConfigurator.configure(testServer.getConfiguration().asMap(), "discarding", ContentStoreInitializer.class))
                .register(FixedThreadPool.class, FixedThreadPool.newInstance(testServer.getConfiguration()));

    }

    // TODO add support for Business-SSL before running this test case
    @Ignore
    @Test
    public void thatWorkerCollectSiriusFlow() throws InterruptedException {
        ExecutionContext context = new ExecutionContext.Builder().services(services).build();

        Headers requestHeaders = new Headers();
        requestHeaders.put("Accept", "application/xml");
        context.globalState(Headers.class, requestHeaders);

        context.variable("fromSequence", 1);

        Flow flow = SiriusFlow.getFlow(testServer.testURL(""));

        Worker worker = new Worker(flow.startNode(), context);
        worker.run();
    }
}
