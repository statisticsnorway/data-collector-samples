package no.ssb.dc.samples.altinn3.test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-override.properties") // gitignored
            .build();

    @Disabled
    @Test
    void getAccessToken() throws NoSuchAlgorithmException, CertificateEncodingException {
        // Load BusinessSSL
        CertificateFactory certificateFactory = CertificateFactory.scanAndCreate(CommonUtils.currentPath());
        CertificateContext certificateContext = certificateFactory.getCertificateContext("ssb-test-certs");
        assertNotNull(certificateContext);

        // Create Java JWT Algorithm using RSA PublicKey and PrivateKey
        X509Certificate cert = certificateContext.trustManager().getAcceptedIssuers()[0];
        KeyPair keyPair = certificateContext.keyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        Algorithm algorithm = Algorithm.RSA256(publicKey, privateKey);
        LOG.trace("algorithm: {}", algorithm);

        // Print Issuer SHA1 Thumbprint
        String thumbprint = DatatypeConverter.printHexBinary(MessageDigest.getInstance("SHA-1").digest(cert.getEncoded())).toLowerCase();
        LOG.trace("thumbprint: {}", thumbprint);

        // Create JWT Header
        Map<String, Object> headerClaims = new HashMap<>();
        headerClaims.put("alg", "RS256");
        headerClaims.put("x5c", List.of(certificateContext.trustManager().getAcceptedIssuers()).stream().map(this::getEncodedCertificate).collect(Collectors.toList()));

        // Create JWT Token and Sign
        String token = JWT.create()
                .withHeader(headerClaims)
                .withAudience("https://ver2.maskinporten.no/")
                .withClaim("resource", "https://tt02.altinn.no/maskinporten-api/")
                .withClaim("scope", "altinn:instances.read altinn:instances.write")
                .withIssuer(configuration.evaluateToString("ssb.altinn.clientId"))
                .withExpiresAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).plusSeconds(30).toInstant()))
                .withIssuedAt(Date.from(Instant.now().atOffset(ZoneOffset.UTC).toInstant()))
                .withJWTId(UUID.randomUUID().toString())
                .sign(algorithm);
        LOG.trace("token: {}", token);

        // Create HttpClient
        Client client = Client.newClient();

        // Request JWT access token from Maskinporten
        String accessToken;
        {
            String payload = String.format("grant_type=%s&assertion=%s", "urn:ietf:params:oauth:grant-type:jwt-bearer", token);

            Request request = Request.newRequestBuilder()
                    .url("https://ver2.maskinporten.no/token")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(payload.getBytes())
                    .build();
            Response response = client.send(request);
            assertEquals(200, response.statusCode(), getHttpError(response)); // VALID JWT REQUEST
            JsonNode jsonNode = JsonParser.createJsonParser().fromJson(new String(response.body(), StandardCharsets.UTF_8), JsonNode.class);
            accessToken = jsonNode.get("access_token").asText();
        }
        String authorizationBearer = String.format("Bearer %s", accessToken);
        LOG.trace("Authorization: {}", authorizationBearer);

        // Replace Maskinporten JWT med Altinn JWT
        {
            Request request = Request.newRequestBuilder()
                    .url("https://platform.tt02.altinn.no/authentication/api/v1/exchange/maskinporten")
                    .header("Authorization", authorizationBearer)
                    .GET()
                    .build();
            Response response = client.send(request);
            assertEquals(200, response.statusCode(), getHttpError(response));
            accessToken = new String(response.body(), StandardCharsets.UTF_8);
            authorizationBearer = String.format("Bearer %s", accessToken);
            LOG.trace("Altinn Authorization: {}", authorizationBearer);
        }

        // Altinn Get Instances
        {
            String url = String.format("https://platform.tt02.altinn.no/storage/api/v1/instances?org=ssb&appId=%s",
                    configuration.evaluateToString("ssb.altinn.app-id"));
            LOG.trace("GetInstanceData-URL: {}", url);
            Request request = Request.newRequestBuilder()
                    .url(url)
                    .header("Accept", "application/json")
                    .header("Authorization", authorizationBearer)
                    .GET()
                    .build();
            Response response = client.send(request);
            assertEquals(200, response.statusCode(), getHttpError(response));
            LOG.trace("GetInstanceData: {}", new String(response.body()));
        }
    }

    byte[] getEncodedCertificate(X509Certificate crt) {
        try {
            return crt.getEncoded();
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    String getHttpError(Response response) {
        return String.format("%s %s", response.statusCode(), HttpStatus.valueOf(response.statusCode()).reason());
    }
}
