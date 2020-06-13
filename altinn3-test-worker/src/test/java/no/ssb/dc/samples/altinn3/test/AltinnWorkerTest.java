package no.ssb.dc.samples.altinn3.test;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Identity;
import no.ssb.dc.api.node.JwtIdentity;
import no.ssb.dc.api.node.Security;
import no.ssb.dc.api.node.builder.JwtBuilder;
import no.ssb.dc.api.node.builder.SecurityBuilder;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.dc.api.Builders.bodyPublisher;
import static no.ssb.dc.api.Builders.claims;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.headerClaims;
import static no.ssb.dc.api.Builders.jqpath;
import static no.ssb.dc.api.Builders.jwt;
import static no.ssb.dc.api.Builders.jwtToken;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.security;
import static no.ssb.dc.api.Builders.status;

public class AltinnWorkerTest {

    static final Logger LOG = LoggerFactory.getLogger(AltinnWorkerTest.class);

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-override.properties") // gitignored
            .values("content.stream.connector", "rawdata")
            .values("rawdata.client.provider", "memory")
            .values("data.collector.worker.threads", "20")
            .build();


    @Disabled
    @Test
    void name() {
        JwtBuilder jwtBuilder = Builders.jwt("test",
                headerClaims()
                        .alg("RS256")
                        .x509CertChain("ssl-test-certs"),
                claims()
                        .audience("aud")
        );
        JwtIdentity jwtIdentity = jwtBuilder.build();

        SecurityBuilder securityBuilder = security();
        securityBuilder.identity(jwtBuilder);
        Security security = securityBuilder.build();

        ExecutionContext context = ExecutionContext.empty();
        context.state(Identity.class, security.identities().get(0));

//        JwtTokenBodyPublisherProducerHandler handler = new JwtTokenBodyPublisherProducerHandler(jwt);
//        handler.execute(context);
    }

    @Disabled
    @Test
    void collect() {
        SpecificationBuilder specificationBuilder = Specification.start("ALTINN-TEST", "Altinn 3", "maskinporten-jwt-grant")
                .configure(security()
                        .identity(jwt("maskinporten",
                                headerClaims()
                                        .alg("RS256")
                                        .x509CertChain("ssb-test-certs"),
                                claims()
                                        .audience("https://ver2.maskinporten.no/")
                                        .issuer("d6a6be6e-a3d9-4ab0-97bf-5c5689dd2a83")
                                        .claim("resource", "https://tt02.altinn.no/maskinporten-api/")
                                        .claim("scope", "altinn:instances.read altinn:instances.write")
                                        .timeToLiveInSeconds(30)
                                )
                        )
                ).function(post("maskinporten-jwt-grant")
                        .url("https://ver2.maskinporten.no/token/api/v1/token")
                        .data(bodyPublisher()
                                .urlEncoded(jwtToken()
                                        .identityId("maskinporten")
                                        .bindTo("JWT_GRANT")
                                        .token("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${JWT_GRANT}")
                                )
                        )
                        .validate(status().success(200))
                        .pipe(execute("altinn-jwt-replacement-token")
                                .inputVariable("accessToken", jqpath(".access_token"))
                        )
                ).function(get("altinn-jwt-replacement-token")
                        .url("https://platform.tt02.altinn.no/authentication/api/v1/exchange/maskinporten")
                        .header("Content-Type", "plain/text")
                        .header("Authorization", "Bearer ${accessToken}")
                        .pipe(execute("loop")
//                                .inputVariable("accessToken", regex("."))
                        )
                ).function(paginate("loop")


                );

        String serialized = specificationBuilder.serialize();
        LOG.trace("{}", serialized);

        Worker.newBuilder()
                .configuration(configuration.asMap())
                .specification(specificationBuilder)
                .buildCertificateFactory(CommonUtils.currentPath())
                .build()
                .run();

    }
}
