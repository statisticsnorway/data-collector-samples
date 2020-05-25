package no.ssb.dc.samples.moveit.bong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.JqPath;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.handler.Queries;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleMoveItTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMoveItTest.class);
    private static final Client client = Client.newClientBuilder().followRedirects(Client.Redirect.ALWAYS).version(Client.Version.HTTP_1_1).build();
    private static final JsonParser jsonParser = JsonParser.createJsonParser();
    private static final byte[] AUTHORIZATION_DATA;

    static {
        final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-ignore.properties")
                .build();
        AUTHORIZATION_DATA = String.format("grant_type=password&username=%s&password=%s", configuration.evaluateToString("username"), configuration.evaluateToString("password")).getBytes();
    }

    static Response GET(String url, String accessToken) {
        Request request = Request.newRequestBuilder()
                .url("https://moveitapitest.ssb.no" + url)
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();

        Response response = client.send(request);
        assertEquals(200, response.statusCode());
        return response;
    }

    static Response POST(String url, byte[] payload) {
        Request request = Request.newRequestBuilder()
                .url("https://moveitapitest.ssb.no" + url)
                .POST(payload)
                .build();

        Response response = client.send(request);
        assertEquals(200, response.statusCode());
        return response;
    }

    static String jq(byte[] data, String expression) {
        JqPath jqPathAccessToken = Builders.jqpath(expression).build();
        QueryFeature jqAccessToken = Queries.from(jqPathAccessToken);
        return jqAccessToken.evaluateStringLiteral(new String(data));
    }

    static String jq(JsonNode node, String expression) {
        JqPath jqPathAccessToken = Builders.jqpath(expression).build();
        QueryFeature jqAccessToken = Queries.from(jqPathAccessToken);
        return jqAccessToken.evaluateStringLiteral(node);
    }

    static JsonNode fromJson(byte[] data) {
        JsonParser jsonParser = JsonParser.createJsonParser();
        return jsonParser.fromJson(new String(data), JsonNode.class);
    }

    static String toPrettyJSON(byte[] data) {
        return jsonParser.toPrettyJSON(fromJson(data));
    }

    static String toPrettyJSON(JsonNode node) {
        return jsonParser.toPrettyJSON(node);
    }

    @Disabled
    @Test
    void flow() {
        Response responseToken = POST("/api/v1/token", AUTHORIZATION_DATA);
        String accessToken = jq(responseToken.body(), ".access_token");

        // --

        Response responseListFiles = GET("/api/v1/files", accessToken);

        JsonNode jsonNode = fromJson(responseListFiles.body());
        //LOG.trace("{}", toPrettyJSON(jsonNode));

        ArrayNode arrayNodeFileItems = (ArrayNode) jsonNode.get("items");
        for (int i = 0; i < arrayNodeFileItems.size(); i++) {
            JsonNode itemNode = arrayNodeFileItems.get(i);
            String fileId = jq(itemNode, ".id");
            String fileName = jq(itemNode, ".name");
            String fileNamePath = jq(itemNode, ".path");
            String fileUploadStamp = jq(itemNode, ".uploadStamp");
            //LOG.trace("id: {}", fileId);

            // get file info

            Response responseFileInfo = GET("/api/v1/files/" + fileId, accessToken);
            JsonNode fileNode = fromJson(responseFileInfo.body());
            //LOG.trace("{}", toPrettyJSON(fileNode));
            String folderId = jq(fileNode, ".folderID");
            //LOG.trace("folderID: {}", folderId);

            Response download = GET(String.format("/api/v1/folders/%s/files/%s/download", folderId, fileId), accessToken);
            LOG.trace("download: {} -> {}", fileNamePath, new String(download.body()));
        }
    }
}
