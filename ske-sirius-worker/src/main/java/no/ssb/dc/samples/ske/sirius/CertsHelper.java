package no.ssb.dc.samples.ske.sirius;

import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.security.SslKeyStore;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CertsHelper {

    static String getCertsResource(String filename) {
        Path certsRootPath = Paths.get(".").resolve("certs");
        String certsPath = certsRootPath.toAbsolutePath().toFile().exists() ?
                certsRootPath.resolve(filename).normalize().toAbsolutePath().toString() :
                Paths.get(".").resolve("data-collector-samples").resolve("ske-sirius-worker").resolve("certs").resolve(filename).normalize().toAbsolutePath().toString();
        return certsPath;
    }


    static SSLContext createSSLContext() {
        StoreBasedDynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource(getCertsResource("secret.properties"))
                .build();

        return SslKeyStore.getSSLContext(
                getCertsResource(configuration.evaluateToString("publicKeyCertificate.file")),
                getCertsResource(configuration.evaluateToString("privateKeyCertificate.file")),
                configuration.evaluateToString("secret.passphrase"),
                "TLSv1.2"
        );
    }
}
