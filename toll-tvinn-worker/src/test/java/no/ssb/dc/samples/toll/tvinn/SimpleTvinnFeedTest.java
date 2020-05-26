package no.ssb.dc.samples.toll.tvinn;

import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleTvinnFeedTest {

    final static Logger LOG = LoggerFactory.getLogger(SimpleTvinnFeedTest.class);
    final static Client client = Client.newClientBuilder().sslContext(getBusinessSSLContext()).build();
    final static String TEST_BASE_URL = "https://api-test.toll.no";

    static SSLContext getBusinessSSLContext() {
        CertificateFactory factory = CertificateFactory.scanAndCreate(CommonUtils.currentPath().getParent());
        CertificateContext context = factory.getCertificateContext("ske-test-certs");
        return context.sslContext();
    }

    Response getPage(String fromMarker, int numberOfEvents) {
        String url = String.format("%s/api/declaration/declaration-clearance-feed/atom?marker=%s&limit=%s&direction=forward", TEST_BASE_URL, fromMarker, numberOfEvents);
        LOG.info("getPage: {}", url);
        Request request = Request.newRequestBuilder()
                .GET()
                .header("content-type", "application/xml")
                .url(url)
                .build();
        return client.send(request);
    }

    @Disabled
    @Test
    void testFeed() {
//        Response response = getPage("last", 1);
        Response response = getPage("edcdbc20-b1db-4fb4-b6fb-20b5bc2b4516", 25);
        LOG.trace("{}", response.headers().asMap());
        assertEquals(200, response.statusCode());
//        LOG.trace("{}", new String(response.body()));
    }
}
