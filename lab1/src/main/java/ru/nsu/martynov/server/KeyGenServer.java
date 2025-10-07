package ru.nsu.martynov.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;


public class KeyGenServer {
    private final int port;
    private final int threads;
    private final String issuerName;
    private final String privateKeyPath;
    private final String certificatePath;

    private ServerGen keyService;
    private ServerConnectionManager connectionManager;
    private Thread connectionThread;

    public KeyGenServer(int port, int threads, String issuerName, String privateKeyPath, String certificatePath) {
        this.port = port;
        this.threads = threads;
        this.issuerName = issuerName;
        this.privateKeyPath = privateKeyPath;
        this.certificatePath = certificatePath;
    }

    public void start() throws Exception {
        // Загрузка приватного ключа и сертификата CA
        PrivateKey issuerPrivateKey = loadPrivateKey(privateKeyPath);
        X509Certificate issuerCert = loadCertificate(certificatePath);
        PublicKey issuerPublicKey = issuerCert.getPublicKey();

        // Создаём сервис генерации ключей
        keyService = new ServerGen(issuerPrivateKey, issuerPublicKey, issuerName, threads);

        // Запускаем сетевой менеджер в отдельном потоке
        connectionManager = new ServerConnectionManager(keyService, port);
        connectionThread = new Thread(() -> {
            try {
                connectionManager.start();
            } catch (Exception e) {
                System.err.println("[Server] ConnectionManager stopped: " + e);
                System.out.flush();
            }
        });
        connectionThread.start();
    }

    public void stop() {
        if (connectionManager != null) {
            connectionManager.stop();
        }
        if (keyService != null) {
            keyService.shutdown();
        }
        if (connectionThread != null) {
            try {
                connectionThread.join(2000);
            } catch (InterruptedException ignored) {}
        }
    }


    private PrivateKey loadPrivateKey(String path) throws Exception {
        // Ожидается PKCS8 PEM без заголовков
        String keyPEM = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)))
                .replaceAll("-----.*?-----", "")
                .replaceAll("\\s", "");
        byte[] keyBytes = Base64.getDecoder().decode(keyPEM);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        return KeyFactory.getInstance("RSA").generatePrivate(spec);
    }

    private X509Certificate loadCertificate(String path) throws Exception {
        try (FileInputStream fis = new FileInputStream(path)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate) cf.generateCertificate(fis);
        }
    }
}
