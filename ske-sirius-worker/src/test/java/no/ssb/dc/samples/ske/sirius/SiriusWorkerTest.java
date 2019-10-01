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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import static org.testng.Assert.assertNotNull;

@Listeners(TestServerListener.class)
public class SiriusWorkerTest {

    private static final Logger LOG = LoggerFactory.getLogger(SiriusWorkerTest.class);

    @Inject
    TestServer testServer;

    Services services;

    @BeforeMethod
    public void setup() {
        Client.Builder clientBuilder = Client.newClientBuilder();
        SSLContext sslContext = CertsHelper.createSSLContext();
        assertNotNull(sslContext);
        clientBuilder.sslContext(sslContext);

        services = Services.create()
                .register(ConfigurationMap.class, new ConfigurationMap(testServer.getConfiguration().asMap()))
                .register(Client.class, clientBuilder.build())
                .register(BufferedReordering.class, new BufferedReordering<>())
                .register(ContentStore.class, ProviderConfigurator.configure(testServer.getConfiguration().asMap(), "discarding", ContentStoreInitializer.class))
                .register(FixedThreadPool.class, FixedThreadPool.newInstance(testServer.getConfiguration()));

    }

    //@Ignore
    @Test
    public void thatWorkerCollectSiriusFlow() throws InterruptedException {
        ExecutionContext context = new ExecutionContext.Builder().services(services).build();

        Headers requestHeaders = new Headers();
        requestHeaders.put("Accept", "application/xml");
        context.globalState(Headers.class, requestHeaders);

        context.variable("baseURL", "https://api-at.sits.no");
        context.variable("fromSequence", 1);

        Flow flow = SiriusFlow.getFlow();

        Worker worker = new Worker(flow.startNode(), context);
        worker.run();
    }
}
