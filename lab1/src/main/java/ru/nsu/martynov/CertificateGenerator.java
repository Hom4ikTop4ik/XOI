package ru.nsu.martynov;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.Date;

public class CertificateGenerator {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static X509Certificate generateSelfSignedCertificate(KeyPair keyPair) throws Exception {
        long now = System.currentTimeMillis();
        Date startDate = new Date(now);
        Date endDate = new Date(now + 10 * 365L * 24 * 60 * 60 * 1000); // 10 лет

        BigInteger serialNumber = BigInteger.valueOf(now);
        X500Name issuer = new X500Name("CN=Test Certificate, O=My Organization, C=RU");
        X500Name subject = new X500Name("CN=Test Certificate, O=My Organization, C=RU");

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer,
                serialNumber,
                startDate,
                endDate,
                subject,
                keyPair.getPublic()
        );

        certBuilder.addExtension(
                Extension.basicConstraints,
                true,
                new BasicConstraints(true)
        );
        certBuilder.addExtension(
                Extension.keyUsage,
                true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign | KeyUsage.cRLSign)
        );
        certBuilder.addExtension(
                Extension.subjectKeyIdentifier,
                false,
                new SubjectKeyIdentifier(keyPair.getPublic().getEncoded())
        );
        KeyPurposeId[] ekuPurposes = new KeyPurposeId[]{
                KeyPurposeId.id_kp_serverAuth,
                KeyPurposeId.id_kp_clientAuth
        };
        certBuilder.addExtension(
                Extension.extendedKeyUsage,
                false,
                new ExtendedKeyUsage(ekuPurposes)
        );

        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA")
                .setProvider("BC")
                .build(keyPair.getPrivate());

        X509CertificateHolder certHolder = certBuilder.build(signer);
        X509Certificate certificate = new JcaX509CertificateConverter()
                .setProvider("BC")
                .getCertificate(certHolder);
        return certificate;
    }

    public static void main(String[] args) {
        try {
            // 1. Генерируем одну пару ключей
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
            keyPairGenerator.initialize(8192, new SecureRandom());
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            // 2. Генерируем сертификат для этой пары
            X509Certificate cert = generateSelfSignedCertificate(keyPair);

            System.out.println("Сертификат успешно создан!");
            System.out.println("Subject: " + cert.getSubjectX500Principal());
            System.out.println("Issuer: " + cert.getIssuerX500Principal());
            System.out.println("Serial Number: " + cert.getSerialNumber());
            System.out.println("Valid From: " + cert.getNotBefore());
            System.out.println("Valid To: " + cert.getNotAfter());
            System.out.println("Signature Algorithm: " + cert.getSigAlgName());

            // Проверка подписи
            cert.verify(cert.getPublicKey());
            System.out.println("\nGOYDA Подпись сертификата валидна!");

            // 3. Сохраняем приватный ключ
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream("ca_private.pem")) {
                fos.write("-----BEGIN PRIVATE KEY-----\n".getBytes());
                fos.write(java.util.Base64.getMimeEncoder(64, "\n".getBytes()).encode(keyPair.getPrivate().getEncoded()));
                fos.write("\n-----END PRIVATE KEY-----\n".getBytes());
            }
            // 4. Сохраняем сертификат
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream("ca_cert.pem")) {
                fos.write("-----BEGIN CERTIFICATE-----\n".getBytes());
                fos.write(java.util.Base64.getMimeEncoder(64, "\n".getBytes()).encode(cert.getEncoded()));
                fos.write("\n-----END CERTIFICATE-----\n".getBytes());
            }
            System.out.println("\nФайлы ca_private.pem и ca_cert.pem успешно созданы!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}