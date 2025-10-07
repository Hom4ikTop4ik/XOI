package ru.nsu.martynov.server;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.Date;

public class GeneratorRSA {

    public static KeyBundle generate(PrivateKey issuerPrivateKey,
                                     PublicKey issuerPublicKey,
                                     String issuerName,
                                     String subjectName) throws Exception {

        // Генерация RSA-ключей
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(8192, new SecureRandom());
        KeyPair keyPair = keyGen.generateKeyPair();



        // Создание. подписание сертификата и прочее
        long now = System.currentTimeMillis();
        Date startDate = new Date(now);
        Date endDate = new Date(now + (3650L * 24 * 60 * 60 * 1000)); // 10 лет

        BigInteger serial = new BigInteger(160, new SecureRandom());

        X500Name issuer = new X500Name("CN=" + issuerName);
        X500Name subject = new X500Name("CN=" + subjectName);

        SubjectPublicKeyInfo subjPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                issuer,
                serial,
                startDate,
                endDate,
                subject,
                subjPubKeyInfo
        );

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .build(issuerPrivateKey);

        X509CertificateHolder holder = certBuilder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter()
                .setProvider("BC") // BouncyCastle 
                .getCertificate(holder);

        return new KeyBundle(keyPair.getPrivate(), keyPair.getPublic(), cert);
    }
}
