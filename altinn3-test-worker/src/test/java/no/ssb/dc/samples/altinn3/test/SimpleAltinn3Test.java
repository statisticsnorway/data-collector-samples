package no.ssb.dc.samples.altinn3.test;

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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * https://difi.github.io/felleslosninger/maskinporten_protocol_jwtgrant.html
 * <p>
 * https://docs.altinn.studio/teknologi/altinnstudio/altinn-api/swagger/storage/#/Instances
 * <p>
 * https://github.com/auth0/java-jwt
 * <p>
 * https://github.com/ks-no/fiks-maskinporten/blob/master/maskinporten-client/src/main/java/no/ks/fiks/maskinporten/Maskinportenklient.java
 * <p>
 * https://github.com/Altinn/altinn-cli/blob/master/AltinnCLI/Commands/Login/SubCommandHandlers/MaskinportenLoginHandler.cs
 * <p>
 * https://github.com/Altinn/altinn-cli/blob/master/AltinnCLI/Commands/Storage/StorageClientWrapper.cs#L45
 */
public class SimpleAltinn3Test {

    static final Logger LOG = LoggerFactory.getLogger(SimpleAltinn3Test.class);
    static final boolean LOG_ACCESS_TOKENS = false;

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-override.properties") // gitignored
            .build();

    static final Client client = Client.newClient();

    @Disabled
    @Test
    void collectAltinnData() {
        String jwtGrant = createMaskinportenJwtGrant(configuration.evaluateToInt("ssb.jwtGrant.expiration"));

        // Request JWT access token from Maskinporten
        String jwtAccessToken = getMaskinportenJwtAccessToken(jwtGrant);

        // Replace Maskinporten JWT med Altinn JWT
        String jwtReplacementToken = getAltinnJwtGrant(jwtAccessToken);

        // Get Instances
        List<JsonNode> instanceList;
        {
            String url = String.format("https://platform.tt02.altinn.no/storage/api/v1/instances?org=ssb&appId=%s", configuration.evaluateToString("ssb.altinn.app-id"));
            LOG.trace("GetInstanceData-URL: {}", url);
            Response response = doGetRequest(url, jwtReplacementToken);
            assertEquals(200, response.statusCode(), getHttpError(response));
            //LOG.trace("GetInstanceData: {}", new String(response.body()));

            instanceList = jqQueryList(response.body(), ".instances[]");
            LOG.trace("instance-list-size: {}", instanceList.size());
        }

        // Get Instance Data
        {
            for (JsonNode instance : instanceList) {
                String ownerPartyId = jqQueryStringLiteral(instance, ".instanceOwner.partyId");
                String instanceId = jqQueryStringLiteral(instance, ".id");
                String url = String.format("https://platform.tt02.altinn.no/storage/api/v1/instances/%s/dataelements", instanceId);
                LOG.trace("Instance-Metadata-URL: {}", url);
                Response response = doGetRequest(url, jwtReplacementToken);
                assertEquals(200, response.statusCode(), getHttpError(response));
                LOG.trace("Instance-Metadata: {} -> {}", instanceId, new String(response.body()));
            }
        }
    }

    String createMaskinportenJwtGrant(int expirationInSeconds) {
        // Load BusinessSSL
        CertificateFactory certificateFactory = CertificateFactory.scanAndCreate(CommonUtils.currentPath());
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
                .withClaim("resource", "https://tt02.altinn.no/maskinporten-api/")
                .withClaim("scope", "altinn:instances.read altinn:instances.write")
                .withIssuer(configuration.evaluateToString("ssb.altinn.clientId"))
                .withExpiresAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).plusSeconds(expirationInSeconds).toInstant()))
                .withIssuedAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).toInstant()))
                .withJWTId(UUID.randomUUID().toString())
                .sign(algorithm);
    }

    String getMaskinportenJwtAccessToken(String jwtGrant) {
        String payload = String.format("grant_type=%s&assertion=%s", "urn:ietf:params:oauth:grant-type:jwt-bearer", jwtGrant);
        Request request = Request.newRequestBuilder()
                .url("https://ver2.maskinporten.no/token")
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

    String getAltinnJwtGrant(String maskinportenJwtAccessToken) {
        Response response = doGetRequest("https://platform.tt02.altinn.no/authentication/api/v1/exchange/maskinporten", maskinportenJwtAccessToken, "plain/text");
        assertEquals(200, response.statusCode(), getHttpError(response));
        String replacementAccessToken = new String(response.body(), StandardCharsets.UTF_8);
        if (LOG_ACCESS_TOKENS) {
            LOG.debug("Replacement-Altinn-AccessToken: {}", replacementAccessToken);
        }
        return replacementAccessToken;
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

    List<JsonNode> jqQueryList(Object data, String expression) {
        return (List<JsonNode>) Queries.from(Builders.jqpath(expression).build()).evaluateList(data);
    }

    Object jqQueryObject(Object data, String expression) {
        return Queries.from(Builders.jqpath(expression).build()).evaluateObject(data);
    }

    String jqQueryStringLiteral(Object data, String expression) {
        return Queries.from(Builders.jqpath(expression).build()).evaluateStringLiteral(data);
    }

    byte[] getEncodedCertificate(X509Certificate crt) {
        try {
            return crt.getEncoded();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    String getHttpError(Response response) {
        String body = new String(response.body(), StandardCharsets.UTF_8);
        return String.format("%s %s%s", response.statusCode(), HttpStatus.valueOf(response.statusCode()).reason(), body.isEmpty() ? "" : String.format("%n%s", body));
    }
}
