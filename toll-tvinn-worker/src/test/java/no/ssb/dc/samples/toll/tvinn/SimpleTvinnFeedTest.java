package no.ssb.dc.samples.toll.tvinn;

import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleTvinnFeedTest {

    final static Logger LOG = LoggerFactory.getLogger(SimpleTvinnFeedTest.class);
    final static String TEST_BASE_URL = "https://api-test.toll.no";

    static SSLContext getBusinessSSLContext() {
//        CertificateFactory factory = CertificateFactory.scanAndCreate(CommonUtils.currentPath().getParent());
        CertificateFactory factory = CertificateFactory.scanAndCreate(Paths.get("/Volumes/SSB BusinessSSL/certs"));
        CertificateContext context = factory.getCertificateContext("ssb-test-certs");
        return context.sslContext();
    }

    /*
curl -H "Authorization: Bearer eyJraWQiOiJjWmswME1rbTVIQzRnN3Z0NmNwUDVGSFpMS0pzdzhmQkFJdUZiUzRSVEQ0IiwiYWxnIjoiUlMyNTYifQ.eyJzY29wZSI6InRvbGw6ZGVjbGFyYXRpb25cL2NsZWFyYW5jZVwvZmVlZC5yZWFkIiwiaXNzIjoiaHR0cHM6XC9cL3ZlcjIubWFza2lucG9ydGVuLm5vXC8iLCJjbGllbnRfYW1yIjoidmlya3NvbWhldHNzZXJ0aWZpa2F0IiwidG9rZW5fdHlwZSI6IkJlYXJlciIsImV4cCI6MTYyMjIwNDk4OSwiaWF0IjoxNjIyMjA0Mzg5LCJjbGllbnRfaWQiOiIzMGNjZmQzNy04NGRkLTQ0NDgtOWM0OC0yM2Q1ODQzMmE2YjEiLCJqdGkiOiIzaUVublVnT3BnVVFCQms1R3dpMGxPTHItc3hyX1cyUTVYeGFRaUlOTEtNIiwiY29uc3VtZXIiOnsiYXV0aG9yaXR5IjoiaXNvNjUyMy1hY3RvcmlkLXVwaXMiLCJJRCI6IjAxOTI6OTcxNTI2OTIwIn19.ovbrD7ParTwDci4YGivdN2sYG3wsQTHbo5zG0w2KPZaN7VysrL8B5lf0bEpKHImyumfdk0ISYR0lChm1e4wSur1UJEdx-PXERDqshPT2dEPDZPELCndjQAw3chRTfJ6EfNNbnFPdbyrDhH8NRc08BI036Mq39-l1apWYhPs9ff9twQZPvR7ZJMfhM6d9vZCJRGaNs7hNx8vgmU0nLJxMfq8qvNNU48KguNrqnUk20hKTnKYtEBh429-a0dnqjvaJQxaBSMmeDBhcccjH3XW6t_f4gs5uzm1XAyEI7Ev5Xd4if89aMP1oXOKko0T3M9P86ml4NCOS_lWMiHaIjYukww" \
    -i "https://api-test.toll.no/api/declaration/declaration-clearance-feed/atom?marker=last&limit=5&direction=forward"

curl -proxy "https://proxy.ssb.no:3128" -H "Authorization: Bearer eyJraWQiOiJjWmswME1rbTVIQzRnN3Z0NmNwUDVGSFpMS0pzdzhmQkFJdUZiUzRSVEQ0IiwiYWxnIjoiUlMyNTYifQ.eyJzY29wZSI6InRvbGw6ZGVjbGFyYXRpb25cL2NsZWFyYW5jZVwvZmVlZC5yZWFkIiwiaXNzIjoiaHR0cHM6XC9cL3ZlcjIubWFza2lucG9ydGVuLm5vXC8iLCJjbGllbnRfYW1yIjoidmlya3NvbWhldHNzZXJ0aWZpa2F0IiwidG9rZW5fdHlwZSI6IkJlYXJlciIsImV4cCI6MTYyMjE5NTk4NiwiaWF0IjoxNjIyMTk1Mzg2LCJjbGllbnRfaWQiOiIzMGNjZmQzNy04NGRkLTQ0NDgtOWM0OC0yM2Q1ODQzMmE2YjEiLCJqdGkiOiJmYnptMHI0NHU3V3pmU0hKSXBwcWRXUDJ2VDJHYWYyXzE0QWYtbHRfd3dnIiwiY29uc3VtZXIiOnsiYXV0aG9yaXR5IjoiaXNvNjUyMy1hY3RvcmlkLXVwaXMiLCJJRCI6IjAxOTI6OTcxNTI2OTIwIn19.eIJSCyOy26eRF4edCo4vggrQyZdx-xZwmQujlZcICDKY5mOHMaA5M12jgUqZMiWt3JGzy6qH8F8fEIrV91ZbSUZP1xg0ZfOb12W8Hgnz62HtIs484LcIioI2Lkgs8TM_QfqutYnHZr1Mlk-_EAOJYZeLv-0kMW67FL4GRE76mBBW3_1qSTSF-109VgKzFQ9NC-r-xmwFr2Pk4LYaWhOkZyrPYHC5SE4gPBNTNMhO7GoxDaZUpqfLbObFpoqBiGElJsmLlSNcXtIl5q2JlirwzLc1TCz_yByVkVIsvivKQmbl1MBJo8eef2oG7zND8seQqkhqPz6MEdeZDIUCl7hmbg" \
    -i "https://api-test.toll.no/api/declaration/declaration-clearance-feed/atom?marker=last&limit=5&direction=forward"

curl -proxy "https://proxy.ssb.no:3128" -H "Authorization: Bearer eyJraWQiOiJjWmswME1rbTVIQzRnN3Z0NmNwUDVGSFpMS0pzdzhmQkFJdUZiUzRSVEQ0IiwiYWxnIjoiUlMyNTYifQ.eyJzY29wZSI6InRvbGw6ZGVjbGFyYXRpb25cL2NsZWFyYW5jZVwvZmVlZC5yZWFkIiwiaXNzIjoiaHR0cHM6XC9cL3ZlcjIubWFza2lucG9ydGVuLm5vXC8iLCJjbGllbnRfYW1yIjoidmlya3NvbWhldHNzZXJ0aWZpa2F0IiwidG9rZW5fdHlwZSI6IkJlYXJlciIsImV4cCI6MTYyMjE5MTM0OSwiaWF0IjoxNjIyMTkwNzQ5LCJjbGllbnRfaWQiOiIzMGNjZmQzNy04NGRkLTQ0NDgtOWM0OC0yM2Q1ODQzMmE2YjEiLCJqdGkiOiJFMzF1MFRWcXNvNkVVbnQzaktNZjh0ZEQtTUd0UHEteUh0NDNHYUFreFpnIiwiY29uc3VtZXIiOnsiYXV0aG9yaXR5IjoiaXNvNjUyMy1hY3RvcmlkLXVwaXMiLCJJRCI6IjAxOTI6OTcxNTI2OTIwIn19.yjnh4FcJPqTk1ONgCX1se0I--cThWbKb-DDAhBGFomkexbtXp49N7hn803j-tqOqYJd5TbQ2Gn8EJ2m6b2dqlV0NJTsU10hwE97raPoaDfeeSPNV9XFxPmje6fcOaOD4smLrQzAOKBxHhFLd8ZYee7DzFVKDtKdw8bfmlBn6lg9C5xBBOJRfKUHcjTga0HNifnRnC-z-8fMvp-SURl4Hx1FcPXM4dbXHTMsM_H1mTbJKZiQrLfyEhjMbk3TVGpyFMudSBq3aBzkzeSv1axJMhB5NHt92PsWGqiSU44oLHhZ5L4yhXn7vbybvSgc2K35NgNBn9ILikZOJyuScO-smaA" -i "https://api-test.toll.no/api/declaration/declaration-clearance-feed/atom?marker=last&limit=5&direction=forward"

     */

    Response getPage(Client client, String fromMarker, int numberOfEvents) {
        String url = String.format("%s/api/declaration/declaration-clearance-feed/atom?marker=%s&limit=%s&direction=forward", TEST_BASE_URL, fromMarker, numberOfEvents);
        LOG.info("getPage: {}", url);
//        String ACCESS_TOKEN = "eyJraWQiOiJjWmswME1rbTVIQzRnN3Z0NmNwUDVGSFpMS0pzdzhmQkFJdUZiUzRSVEQ0IiwiYWxnIjoiUlMyNTYifQ.eyJzY29wZSI6InRvbGw6ZGVjbGFyYXRpb25cL2NsZWFyYW5jZVwvZmVlZC5yZWFkIiwiaXNzIjoiaHR0cHM6XC9cL3ZlcjIubWFza2lucG9ydGVuLm5vXC8iLCJjbGllbnRfYW1yIjoidmlya3NvbWhldHNzZXJ0aWZpa2F0IiwidG9rZW5fdHlwZSI6IkJlYXJlciIsImV4cCI6MTYyMjE4MTUzMSwiaWF0IjoxNjIyMTgwOTMxLCJjbGllbnRfaWQiOiIzMGNjZmQzNy04NGRkLTQ0NDgtOWM0OC0yM2Q1ODQzMmE2YjEiLCJqdGkiOiJGWTluTjFaTGVTQVg0S0Y3dzJlSXZQaC1Gbi1QLV80b01vMjBfVDJrbFl3IiwiY29uc3VtZXIiOnsiYXV0aG9yaXR5IjoiaXNvNjUyMy1hY3RvcmlkLXVwaXMiLCJJRCI6IjAxOTI6OTcxNTI2OTIwIn19.r_J--e_arEqnt-GgZHTJr7jWbE306vY8usNJcSzNU2-IMhSdR2zfEVpX_9-HmpGp01W4ijI9Akzj8TGybYqIIOyoDa09iyheG4BKtCVfxWr0x9H-isEvVyTCkLFS9Ijv0oAmmr1qAQwhSnrJ-g7s13EVeEr6bwo7S5ML3x-1RBjLtn5iE46YUU0IcPkolvJ2e_MdM-yPtcTixdSbM5ewsCIR7SGNuUXZBFA19sxI7OsfWZ6vtNyhqJZF84K8Au-UhjXbbSFal2ckXYdkXVDTyNVMVhq0DklFA1Kx6d_LD31Qxpp31YldoylETC6o4Lvu2GGEd-TxIre8Jb1JCIHXTg";
        String ACCESS_TOKEN = "eyJraWQiOiJjWmswME1rbTVIQzRnN3Z0NmNwUDVGSFpMS0pzdzhmQkFJdUZiUzRSVEQ0IiwiYWxnIjoiUlMyNTYifQ.eyJzY29wZSI6InRvbGw6ZGVjbGFyYXRpb25cL2NsZWFyYW5jZVwvZmVlZC5yZWFkIiwiaXNzIjoiaHR0cHM6XC9cL3ZlcjIubWFza2lucG9ydGVuLm5vXC8iLCJjbGllbnRfYW1yIjoidmlya3NvbWhldHNzZXJ0aWZpa2F0IiwidG9rZW5fdHlwZSI6IkJlYXJlciIsImV4cCI6MTYyMjE5MDY5MywiaWF0IjoxNjIyMTkwMDkzLCJjbGllbnRfaWQiOiIzMGNjZmQzNy04NGRkLTQ0NDgtOWM0OC0yM2Q1ODQzMmE2YjEiLCJqdGkiOiI0cV9jSktnMmJMTE9ZM1R4azM5Y2gybUpaSkR4VnlLVUs2TVlRRGJ5TmpBIiwiY29uc3VtZXIiOnsiYXV0aG9yaXR5IjoiaXNvNjUyMy1hY3RvcmlkLXVwaXMiLCJJRCI6IjAxOTI6OTcxNTI2OTIwIn19.0oCRFbH7NbJQLaRG8b_1gGApkC1HXuN9A0j7BCYNNghhgajp4mE6vdlLd9dflZ6EuNbw1xhpaowL0hUHlnEW2yssUzAWGKa8dwaj8tkP5hul48u1E_NJ2MkE7hGVOtx58C7Gv8f-CVVGK6uMBsBenC9O9pfGg2hQACQQwYgAUD6HDvHQzWg5PG7huL-XZc67uWSt35ML7It4f_NTSzI_3WgmVC1fH79AIZGjTt8wBEocoML46j7TYpDbB1MfurHUNLoyrV34-SsKwYhh77dTjaJyeERA6ziyoEcT6dDTMuW64YBapJiTB0jlXa4C49qXI7zTGZp2sT5FuIuf__8wpA";
        Request request = Request.newRequestBuilder()
                .GET()
                .header("Authorization", String.format("Bearer %s", ACCESS_TOKEN))
                .header("Content-Type", "application/xml")
                .url(url)
                .build();
        return client.send(request);
    }

    @Disabled
    @Test
    void testFeed() {
        Client client = Client.newClientBuilder().sslContext(getBusinessSSLContext()).build();
//        Response response = getPage("last", 1);
        Response response = getPage(client, "last", 5);
        LOG.info("body: {}", new String(response.body()));
        assertEquals(200, response.statusCode());
//        LOG.trace("{}", new String(response.body()));
    }
}
