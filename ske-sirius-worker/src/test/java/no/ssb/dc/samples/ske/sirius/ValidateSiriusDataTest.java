package no.ssb.dc.samples.ske.sirius;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.handler.Queries;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.of;

public class ValidateSiriusDataTest {

    static final Logger LOG = LoggerFactory.getLogger(ValidateSiriusDataTest.class);

    static final String TARGET = "utkast";
    //static final String TARGET = "fastsatt";
    static final String TOPIC = "sirius-person-" + TARGET;

    @Disabled
    @Test
    void validate() throws Exception {
        Path targetPath = CommonUtils.currentPath().resolve("target").resolve("sirius").resolve(TARGET);
        if (!targetPath.toFile().exists()) {
            LOG.trace("Error resolving targetPath: {}", targetPath);
            return;
        }

        /*
         * Create rawdata client
         */
        RawdataClient rawdataClient = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
        RawdataProducer producer = rawdataClient.producer(TOPIC);

        /*
         * Cache Hendelseliste
         */
        Map<String, Hendelse> hendelseListe = new LinkedHashMap<>();
        Files.walkFileTree(targetPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String filename = file.getFileName().toString();
                if (filename.contains("hendelse-liste")) {
                    byte[] hendelseListeContent = Files.readAllBytes(file);
                    Optional<Element> documentElement = DOM.documentElement(hendelseListeContent);
                    List<Element> elements = documentElement.map(element -> element.getElementsByTagName("hendelse")).flatMap(DOM::allElements).get();
                    for (Element element : elements) {
                        Hendelse hendelse = new Hendelse(element, SimpleSiriusFeedTest.XML.serialize(element));
                        hendelseListe.put(hendelse.sekvensnummer, hendelse);
                    }
                }
                return super.visitFile(file, attrs);
            }
        });

        /*
         * Publish rawdata stream
         */
        List<String> positions = new ArrayList<>();
        Files.walkFileTree(targetPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String filename = file.getFileName().toString();
                if (filename.contains("skatte-melding") && filename.endsWith(".xml")) {
                    String position = String.valueOf(Integer.parseInt(file.getFileName().toString().split("-")[0]));

                    String manifestFilenamePath = file.toString().replace("skatte-melding", "skatte-melding-manifest").replace(".xml", ".json");
                    byte[] manifestContent = Files.readAllBytes(Paths.get(manifestFilenamePath));
                    byte[] skattemeldingContent = Files.readAllBytes(file);

                    RawdataMessage.Builder builder = producer.builder();
                    builder.position(position);
                    builder.put("manifest.json", manifestContent);
                    builder.put("hendelse", hendelseListe.get(position).content);
                    builder.put("skattemelding", skattemeldingContent);
                    producer.buffer(builder);

                    positions.add(position);
                }
                return super.visitFile(file, attrs);
            }
        });
        producer.publish(positions.stream().map(Integer::parseInt).sorted().map(String::valueOf).toArray(String[]::new));

        /*
         * TODO Create and prepare database
         */



        /*
         * Consume rawdata stream and persist
         */
        try (RawdataConsumer consumer = rawdataClient.consumer(TOPIC)) {
            RawdataMessage rawdataMessage;
            while ((rawdataMessage = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                byte[] manifestContent = rawdataMessage.get("manifest.json");
                byte[] content = rawdataMessage.get("skattemelding");

                String manifestJson = new String(manifestContent);

                String position = Queries.from(Builders.jqpath(".metadata.position").build()).evaluateStringLiteral(manifestJson);
                String url = Queries.from(Builders.jqpath(".metadata.url").build()).evaluateStringLiteral(manifestJson);
                String statusCode = Queries.from(Builders.jqpath(".\"http-info\".statusCode").build()).evaluateStringLiteral(manifestJson);

                Hendelse hendelse = hendelseListe.get(position);
                Manifest manifest = new Manifest(position, url, Integer.parseInt(statusCode));
                Skattemelding skattemelding = new Skattemelding(content);

                /*
                 * any deviance between: hendelse.registreringstidspunkt AND skattemelding.registreringstidspunkt
                 */
                if (!skattemelding.skjermet && !hendelse.registreringstidspunkt.equals(skattemelding.registreringstidspunkt)) {
                    LOG.error("position: {} hendelse-timestamp: {} NOT EQUAL to skattemelding-timstamp: {}", position, hendelse.registreringstidspunkt, skattemelding.registreringstidspunkt);
                    LOG.error("Exiting!");
                    System.exit(-1);
                }

                /*
                 * is skattemelding skjermet
                 */
                if (skattemelding.skjermet) {
                    LOG.error("isSkjermet: {}\n| {}\n+-| {}\n  +--> {}", skattemelding.skjermet, hendelse, manifest, skattemelding);
                } else {
                    LOG.info("isSkjermet: {}\n| {}\n+-| {}\n  +--> {}", skattemelding.skjermet, hendelse, manifest, skattemelding);
                }

                /*
                 * any non 200 status codes
                 */
                if (manifest.statusCode != 200) {
                    throw new RuntimeException(String.format("Position: %s returned http error: %s", manifest.position, manifest.statusCode));
                }
            }
        }
    }

    static class DOM {
        static Optional<Element> documentElement(byte[] content) {
            return of(SimpleSiriusFeedTest.XML.deserialize(content).getDocumentElement());
        }

        static Optional<Element> firstElement(NodeList nodeList) {
            return nodeList.getLength() > 0 ? of((Element) nodeList.item(0)) : Optional.empty();
        }

        static Optional<String> firstElementAsValue(NodeList nodeList) {
            return nodeList.getLength() > 0 ? of(nodeList.item(0).getTextContent()) : Optional.empty();
        }

        static Optional<Date> firstElementFromUTCAsDate(NodeList nodeList) {
            return firstElement(nodeList)
                    .flatMap(element -> of(element.getTextContent()))
                    .flatMap(value -> of(DateTimeFormatter.ISO_DATE_TIME.parse(value))
                            .map(accessor -> Date.from(Instant.from(accessor))));
        }

        public static Optional<List<Element>> allElements(NodeList nodeList) {
            List<Element> elements = new ArrayList<>();
            for (int i = 0; i < nodeList.getLength(); i++) {
                elements.add((Element) nodeList.item(i));
            }
            return of(elements);
        }
    }

    static class Hendelse {
        final String sekvensnummer;
        final String identifikator;
        final String gjelderPeriode;
        final Date registreringstidspunkt;
        final String hendelsetype;
        final byte[] content;

        public Hendelse(Element element, byte[] content) {
            this.sekvensnummer = of(element).map(child -> child.getElementsByTagName("sekvensnummer")).flatMap(DOM::firstElementAsValue).orElseThrow();
            this.identifikator = of(element).map(child -> child.getElementsByTagName("identifikator")).flatMap(DOM::firstElementAsValue).orElseThrow();
            this.gjelderPeriode = of(element).map(child -> child.getElementsByTagName("gjelderPeriode")).flatMap(DOM::firstElementAsValue).orElseThrow();
            this.registreringstidspunkt = of(element).map(child -> child.getElementsByTagName("registreringstidspunkt")).flatMap(DOM::firstElementFromUTCAsDate).orElseThrow();
            this.hendelsetype = of(element).map(child -> child.getElementsByTagName("hendelsetype")).flatMap(DOM::firstElementAsValue).orElseThrow();
            this.content = content;
        }

        static String formatDateTime(Date registreringstidspunkt) {
            if (registreringstidspunkt == null) {
                return null;
            }
            return registreringstidspunkt.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().toString();
        }

        @Override
        public String toString() {
            return "Hendelse{" +
                    "sekvensnummer='" + sekvensnummer + '\'' +
                    ", identifikator='" + identifikator + '\'' +
                    ", gjelderPeriode='" + gjelderPeriode + '\'' +
                    ", registreringstidspunkt=" + formatDateTime(registreringstidspunkt) +
                    ", hendelsetype='" + hendelsetype + '\'' +
                    '}';
        }
    }

    static class Manifest {
        private final String position;
        private final String url;
        private final int statusCode;

        public Manifest(String position, String url, int statusCode) {
            this.position = position;
            this.url = url;
            this.statusCode = statusCode;
        }

        @Override
        public String toString() {
            return "Manifest{" +
                    "position='" + position + '\'' +
                    ", url='" + url + '\'' +
                    ", statusCode=" + statusCode +
                    '}';
        }
    }

    static class Skattemelding {
        final String identifikator;
        final boolean skjermet;
        final Date registreringstidspunkt;
        final String feilMelding;

        public Skattemelding(byte[] content) {
            Optional<Element> root = DOM.documentElement(content);
            this.identifikator = root.map(element -> element.getElementsByTagName("personidentifikator")).flatMap(DOM::firstElementAsValue).orElseThrow();
            this.skjermet = root.map(element -> element.getElementsByTagName("skjermet")).flatMap(DOM::firstElementAsValue).map(Boolean::parseBoolean).orElse(false);
            this.registreringstidspunkt = root.map(element -> element.getElementsByTagName("registreringstidspunkt")).flatMap(DOM::firstElementFromUTCAsDate).orElse(null);
            this.feilMelding = root.map(element -> element.getElementsByTagName("feil")).flatMap(DOM::firstElementAsValue).orElse(null);
        }

        @Override
        public String toString() {
            return "Skattemelding{" +
                    "identifikator='" + identifikator + '\'' +
                    ", skjermet=" + skjermet +
                    ", registreringstidspunkt=" + Hendelse.formatDateTime(registreringstidspunkt) +
                    ", feilMelding='" + feilMelding + '\'' +
                    '}';
        }
    }

}
