package ru.nsu.martynov.server;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

public class KeyBundle {
    private final PrivateKey privateKey;
    private final PublicKey publicKey;
    private final X509Certificate cert;

    public KeyBundle(PrivateKey privateKey, PublicKey publicKey, X509Certificate cert) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.cert = cert;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    public X509Certificate getCertificate() {
        return cert;
    }
}
