package no.ssb.dc.samples.moveit.bong;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.dc.api.Builders.bodyPublisher;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.jqpath;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;

public class MoveItTest {

    static final Logger LOG = LoggerFactory.getLogger(MoveItTest.class);

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-ignore.properties")
            .values("content.stream.connector", "rawdata")
            .values("rawdata.client.provider", "memory")
            .values("data.collector.worker.threads", "20")
            .values("data.collector.http.version", "HTTP_1_1")
            .build();

    SpecificationBuilder createSpecification(String serviceURL) {
        return Specification.start("MOVEIT", "MoveIt", "authorize")
                .configure(context()
                        .topic("moveit-bong-test")
                )
                .function(post("authorize")
                        .url(serviceURL + "/api/v1/token")
                        .data(bodyPublisher()
                                .plainText("grant_type=password&username=${ENV.moveIt_server_username}&password=${ENV.moveIt_server_password}")
                        )
                        .validate(status().success(200, 299))
                        .pipe(execute("loop")
                                .inputVariable("accessToken", jqpath(".access_token"))
                        )
                )
                .function(paginate("loop")
                        .iterate(execute("page")
                                .requiredInput("accessToken")
                        )
                        .prefetchThreshold(150)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .header("Accept", "application/xml")
                        .header("Authorization", "Bearer ${accessToken}")
                        .url(serviceURL + "/api/v1/files")
                        .pipe(sequence(jqpath(".items"))
                                .expected(jqpath(".id"))
                        )
                );

    }

    @Disabled
    @Test
    void consumeMoveItFiles() {
        LOG.trace("asMap:\n{}", configuration.asMap());
        SpecificationBuilder spec = createSpecification(configuration.evaluateToString("moveIt_server_url"));
        Worker.newBuilder()
                .configuration(configuration.asMap())
                .specification(spec)
                .build()
                .run();
    }
}
