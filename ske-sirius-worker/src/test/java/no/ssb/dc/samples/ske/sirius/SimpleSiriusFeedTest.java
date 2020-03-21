package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.handler.QueryException;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.FixedThreadPool;
import no.ssb.dc.core.security.CertificateContext;
import no.ssb.dc.core.security.CertificateFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

// https://skatteetaten.github.io/datasamarbeid-api-dokumentasjon/reference_skattemelding
class SimpleSiriusFeedTest {

    final static Logger LOG = LoggerFactory.getLogger(SimpleSiriusFeedTest.class);
    final static Client client = Client.newClientBuilder().sslContext(getBusinessSSLContext()).build();
    final static String TEST_BASE_URL = "https://api-at.sits.no";

    static SSLContext getBusinessSSLContext() {
        CertificateFactory factory = CertificateFactory.scanAndCreate(CommonUtils.currentPath());
        CertificateContext context = factory.getCertificateContext("ske-test-certs");
        return context.sslContext();
    }

    Response getHendelseListe(Hendelse hendelse, String fromSequence, int numberOfEvents) {
        Request request = Request.newRequestBuilder()
                .GET()
                .header("accept", "application/xml")
                .url(String.format("%s/api/formueinntekt/skattemelding/%s/hendelser/?fraSekvensnummer=%s&antall=%s", TEST_BASE_URL, hendelse.value, fromSequence, numberOfEvents))
                .build();
        return client.send(request);
    }

    CompletableFuture<Response> getSkattemelding(Hendelse hendelse, String identifier, String incomeYear, String snapshot) {
        Request request = Request.newRequestBuilder()
                .GET()
                .header("accept", "application/xml")
                .url(String.format("%s/api/formueinntekt/skattemelding/%s/ssb/%s/%s?gjelderPaaTidspunkt=%s", TEST_BASE_URL, hendelse.value, incomeYear, identifier, snapshot))
                .build();
        return client.sendAsync(request);
    }

    @Disabled
    @ParameterizedTest
    @EnumSource(Hendelse.class)
    void getHendelseListe(Hendelse hendelse) throws ExecutionException, InterruptedException {
        FixedThreadPool threadPool = FixedThreadPool.newInstance(20);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<SkattemeldingResponse> responseList = new CopyOnWriteArrayList<>();

        FileWriter fileWriter = new FileWriter(CommonUtils.currentPath().resolve("target").resolve("sirius").resolve(hendelse.value));
        fileWriter.writeSkatteMeldinger("---------\n");

        LOG.trace("BEGIN");
        Response hendelseListeResponse = getHendelseListe(hendelse, "1", 500);
        assertEquals(200, hendelseListeResponse.statusCode());
        Document doc = XML.deserialize(hendelseListeResponse.body());
        fileWriter.writeHendelseListe(XML.toPrettyXML(doc));

        NodeList hendelser = doc.getDocumentElement().getElementsByTagName("hendelse");
        for (int i = 0; i < hendelser.getLength(); i++) {
            Element element = (Element) hendelser.item(i);

            String sekvensnummer = element.getElementsByTagName("sekvensnummer").item(0).getTextContent();
            String identifikator = element.getElementsByTagName("identifikator").item(0).getTextContent();
            String gjelderPeriode = element.getElementsByTagName("gjelderPeriode").item(0).getTextContent();
            String registreringstidspunkt = element.getElementsByTagName("registreringstidspunkt").item(0).getTextContent();

            CompletableFuture<Void> requestFuture = CompletableFuture
                    .supplyAsync(() -> {
                        LOG.trace("Get Skattemelding: {}", sekvensnummer);
                        getSkattemelding(hendelse, identifikator, gjelderPeriode, registreringstidspunkt)
                                .thenApply(response -> {
                                    //assertEquals(200, skattemeldingResponse.statusCode());
                                    responseList.add(new SkattemeldingResponse(sekvensnummer, response));
                                    return response;
                                }).join();
                        return null;
                    }, threadPool.getExecutor());

            futures.add(requestFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(ignore -> {
                    responseList.sort(Comparator.comparing(sr -> Integer.parseInt(sr.sekvensnummer)));
                    for (SkattemeldingResponse sr : responseList) {
                        LOG.trace("Write Skattemelding: {}", sr.sekvensnummer);
                        fileWriter.writeSkatteMeldinger(String.format("Sekvensnummer: %s%n", sr.sekvensnummer));
                        fileWriter.writeSkatteMeldinger(String.format("URL: %s%n", sr.response.url()));
                        fileWriter.writeSkatteMeldinger(XML.toPrettyXML(sr.response.body()));
                        fileWriter.writeSkatteMeldinger("---------\n");
                    }
                    return null;
                }).join();

        LOG.trace("END");
    }

    enum Hendelse {
        UTKAST("utkast"),
        FASTSATT("fastsatt");

        private final String value;

        Hendelse(String value) {
            this.value = value;
        }
    }

    static class SkattemeldingResponse {
        final String sekvensnummer;
        final Response response;

        public SkattemeldingResponse(String sekvensnummer, Response response) {
            this.sekvensnummer = sekvensnummer;
            this.response = response;
        }
    }

    static class FileWriter {
        private final Path outputPath;
        private final Path hendelseListeFile;
        private final Path skattemeldingerFile;

        FileWriter(Path outputPath) {
            this.outputPath = outputPath;
            String fileTimestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            this.hendelseListeFile = outputPath.resolve(String.format("hendelse-liste-%s.xml", fileTimestamp));
            this.skattemeldingerFile = outputPath.resolve(String.format("skatte-meldinger-%s.xml", fileTimestamp));
            LOG.trace("OutputPath: {}", outputPath.normalize().toString());
            try {
                Files.createDirectories(outputPath);
                Files.write(hendelseListeFile, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.write(skattemeldingerFile, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void writeHendelseListe(String xml) {
            try {
                BufferedWriter writer = Files.newBufferedWriter(hendelseListeFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                writer.write(xml);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


        void writeSkatteMeldinger(String xml) {
            try {
                BufferedWriter writer = Files.newBufferedWriter(skattemeldingerFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                writer.write(xml);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class XML {
        static byte[] serialize(Object document) {
            try (StringWriter writer = new StringWriter()) {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
                transformer.transform(new DOMSource((Node) document), new StreamResult(writer));
                return writer.toString().getBytes();
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new QueryException(e);
            }
        }

        static Document deserialize(byte[] source) {
            try {
                SAXParserFactory sax = SAXParserFactory.newInstance();
                sax.setNamespaceAware(false);
                XMLReader reader = sax.newSAXParser().getXMLReader();
                try (ByteArrayInputStream bais = new ByteArrayInputStream(source)) {
                    SAXSource saxSource = new SAXSource(reader, new InputSource(bais));
                    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                    documentBuilderFactory.setNamespaceAware(false);
                    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
                    Document doc = documentBuilder.parse(saxSource.getInputSource());
                    doc.normalizeDocument();
                    return doc;
                }
            } catch (SAXException | IOException | ParserConfigurationException e) {
                throw new QueryException(new String(source), e);
            }
        }

        static String toPrettyXML(Node node) {
            return new String(serialize(node), StandardCharsets.UTF_8);
        }

        static String toPrettyXML(byte[] bytes) {
            return toPrettyXML(deserialize(bytes));
        }

        static String toPrettyXML(String xml) {
            return toPrettyXML(deserialize(xml.getBytes()));
        }
    }

}
