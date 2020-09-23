package no.ssb.dc.samples.ske.freg;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.handler.Queries;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleFregMaskinportenTest {

    static final Logger LOG = LoggerFactory.getLogger(SimpleFregMaskinportenTest.class);
    static final boolean LOG_ACCESS_TOKENS = false;

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-override.properties") // gitignored
            .build();

    static final Client client = Client.newClient();
    private static Path contentStorePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        contentStorePath = CommonUtils.currentPath().resolve("target").resolve("store");
        if (!contentStorePath.toFile().exists()) {
            Files.createDirectories(contentStorePath);
        }
    }

    @Disabled
    @Test
    public void collectFregHendelseliste() throws IOException {
        String jwtGrant = createMaskinportenJwtGrant(configuration.evaluateToInt("ssb.jwtGrant.expiration"));
        LOG.trace("jwtGrant: {}", jwtGrant);

        String jwtAccessToken = getMaskinportenJwtAccessToken(jwtGrant);
        LOG.trace("jwtAccessToken: {}", jwtGrant);

        {
            String url = "https://folkeregisteret-api-konsument.sits.no/folkeregisteret/api/brsv/v1/hendelser/feed?seq=1";
            LOG.trace("GetHendelseliste-URL: {}", url);
            Response response = doGetRequest(url, jwtAccessToken);
            assertEquals(200, response.statusCode(), getHttpError(response));
            LOG.trace("GetResponse: {}", new String(response.body()));
        }
    }

    byte[] getEncodedCertificate(X509Certificate crt) {
        try {
            return crt.getEncoded();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    Response doGetRequest(String url, String token) {
        return doGetRequest(url, token, null);
    }

    Response doGetRequest(String url, String token, String contentType) {
        Request request = Request.newRequestBuilder()
                .url(url)
                .header("Accept", contentType == null ? "application/json" : contentType)
                .header("Authorization", String.format("Bearer %s", token))
                .GET()
                .build();
        return client.send(request);
    }

    String getHttpError(Response response) {
        String body = new String(response.body(), StandardCharsets.UTF_8);
        return String.format("%s [%s] %s%s", response.url(), response.statusCode(), HttpStatus.valueOf(response.statusCode()).reason(), body.isEmpty() ? "" : String.format("%n%s", body));
    }

    List<JsonNode> jqQueryList(Object data, String expression) {
        return (List<JsonNode>) Queries.from(Builders.jqpath(expression).build()).evaluateList(data);
    }

    Object jqQueryObject(Object data, String expression) {
        return Queries.from(Builders.jqpath(expression).build()).evaluateObject(data);
    }

    String jqQueryStringLiteral(Object data, String expression) {
        return Queries.from(Builders.jqpath(expression).build()).evaluateStringLiteral(data);
    }

    String createMaskinportenJwtGrant(int expirationInSeconds) {
        // Load BusinessSSL
        Path certStorePath = CommonUtils.currentPath().resolve("pkcs12-certs");
        assertTrue(certStorePath.toFile().exists());
        CertificateFactory certificateFactory = CertificateFactory.scanAndCreate(certStorePath);
        CertificateContext certificateContext = certificateFactory.getCertificateContext("ssb-test-certs");
        assertNotNull(certificateContext);

        // Create Java JWT Algorithm using RSA PublicKey and PrivateKey
        KeyPair keyPair = certificateContext.keyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        Algorithm algorithm = Algorithm.RSA256(publicKey, privateKey);

        // Print Issuer SHA1 Thumbprint
        //String thumbprint = DatatypeConverter.printHexBinary(MessageDigest.getInstance("SHA-1").digest(cert.getEncoded())).toLowerCase();
        //LOG.trace("thumbprint: {}", thumbprint);

        // Create JWT Header
        Map<String, Object> headerClaims = new HashMap<>();
        headerClaims.put("alg", "RS256");
        headerClaims.put("x5c", List.of(certificateContext.trustManager().getAcceptedIssuers()).stream().map(this::getEncodedCertificate).collect(Collectors.toList()));

        // Create JWT Grant and Sign
        return JWT.create()
                .withHeader(headerClaims)
                .withAudience("https://ver2.maskinporten.no/")
                .withClaim("scope", "folkeregister:deling/svalbardregister folkeregister:deling/offentligmedhjemmel")
                .withIssuer(configuration.evaluateToString("ssb.ske.freg.clientId"))
                .withExpiresAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).plusSeconds(expirationInSeconds).toInstant()))
                .withIssuedAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).toInstant()))
                .withJWTId(UUID.randomUUID().toString())
                .sign(algorithm);
    }

    String getMaskinportenJwtAccessToken(String jwtGrant) {
        String payload = String.format("grant_type=%s&assertion=%s", "urn:ietf:params:oauth:grant-type:jwt-bearer", jwtGrant);
        Request request = Request.newRequestBuilder()
                .url("https://ver2.maskinporten.no/token")
//                .header("Accept", "application/xml")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(payload.getBytes())
                .build();
        Response response = client.send(request);
        assertEquals(200, response.statusCode(), getHttpError(response));
        JsonNode jsonNode = JsonParser.createJsonParser().fromJson(new String(response.body(), StandardCharsets.UTF_8), JsonNode.class);
        String accessToken = jsonNode.get("access_token").asText();
        if (LOG_ACCESS_TOKENS) {
            LOG.debug("Acquired-Maskinporten-AccessToken: {}", accessToken);
        }
        return accessToken;
    }


}
