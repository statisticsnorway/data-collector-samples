package no.ssb.dc.samples.moveit.bong;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.core.executor.Worker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.bodyPublisher;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.eval;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.jqpath;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenExpressionIsTrue;

public class MoveItTest {

    static final Logger LOG = LoggerFactory.getLogger(MoveItTest.class);

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-ignore.properties")
            .values("content.stream.connector", "rawdata")
            .values("rawdata.client.provider", "memory")
            .values("data.collector.worker.threads", "20")
            .values("data.collector.http.version", "HTTP_1_1")
            .values("data.collector.http.followRedirects", Client.Redirect.ALWAYS.name())
            .build();

    SpecificationBuilder createSpecification(String serverURL) {
        return Specification.start("MOVEIT", "MoveIt", "authorize")
                .configure(context()
                        .topic("moveit-bong-test")
                        .variable("baseURL", serverURL)
                        .variable("rootFolder", "/Home/moveitapi")
                        .variable("nextPage", "${cast.toLong(contentStream.lastOrInitialPosition(0)) + 1}")
                )
                .function(post("authorize")
                        .url("${baseURL}/api/v1/token")
                        .data(bodyPublisher()
                                .plainText("grant_type=password&username=${ENV.moveIt_server_username}&password=${ENV.moveIt_server_password}")
                        )
                        .validate(status().success(200))
                        .pipe(execute("find-root-folder")
                                .inputVariable("accessToken", jqpath(".access_token"))
                        )
                )
                .function(get("find-root-folder")
                        .header("Authorization", "Bearer ${accessToken}")
                        .url("${baseURL}/api/v1/folders")
                        .validate(status().success(200))
                        .pipe(execute("loop")
                                .requiredInput("accessToken")
                                .inputVariable("folderId", jqpath(".items[] | select(.path == \"${rootFolder}\") | .id"))
                        )
                )
                .function(paginate("loop")
                        .variable("fromPage", "${nextPage}")
                        .addPageContent("fromPage")
                        .iterate(execute("page")
                                .requiredInput("accessToken")
                                .requiredInput("folderId")
                        )
                        .prefetchThreshold(150)
                        .until(whenExpressionIsTrue("${nextPage >= totalPages}"))
                )
                .function(get("page")
                        .header("Authorization", "Bearer ${accessToken}")
                        .url("${baseURL}/api/v1/folders/${folderId}/files?page=${nextPage}&sortDirection=asc&sortField=uploadStamp")
                        .validate(status().success(200))
                        .pipe(sequence(jqpath(".items[]"))
                                .expected(jqpath(".id"))
                        )
                        .pipe(nextPage()
                                .output("nextPage",
                                        eval(jqpath(".paging.page"), "lastPage", "${cast.toLong(lastPage) + 1}")
                                )
                                .output("totalPages", jqpath(".paging.totalPages"))
                        )
                        .pipe(parallel(jqpath(".items[]"))
                                .variable("position", jqpath(".id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(execute("download-file")
                                        .requiredInput("folderId")
                                        .inputVariable("name", jqpath(".name"))
                                        .inputVariable("path", jqpath(".path"))
                                        .inputVariable("uploadStamp", jqpath(".uploadStamp"))
                                )
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPage", "totalPages")
                )
                .function(get("download-file")
                        .header("Authorization", "Bearer ${accessToken}")
                        .url("${baseURL}/api/v1/folders/${folderId}/files/${position}/download")
                        .validate(status().success(200, 299))
                        .pipe(addContent("${position}", "file"))
                );
    }

    @Disabled
    @Test
    void consumeMoveItFiles() {
        Worker.newBuilder()
                .configuration(configuration.asMap())
                .specification(createSpecification(configuration.evaluateToString("moveIt_server_url")))
                .build()
                .run();
    }
}
