package ru.nsu.martynov.server;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class ServerGen {

    private final Map<String, KeyBundle> keysMap = new HashMap<>();

    private final PrivateKey issuerPrivateKey;
    private final PublicKey issuerPublicKey;
    private final String issuerName;

    private final ExecutorService generatorPool;

    public ServerGen(PrivateKey issuerPrivateKey, PublicKey issuerPublicKey, String issuerName, int threads) {
        this.issuerPrivateKey = issuerPrivateKey;
        this.issuerPublicKey = issuerPublicKey;
        this.issuerName = issuerName;
        this.generatorPool = Executors.newFixedThreadPool(threads);

        // Регистрация BouncyCastle
        Security.addProvider(new BouncyCastleProvider());
    }

    public void getOrCreateKeysAsync(String name, Consumer<KeyBundle> callback) {
        boolean needToGenerate = false;

        synchronized (keysMap) {
            if (!keysMap.containsKey(name)) {
                keysMap.put(name, null); // заглушка
                needToGenerate = true;
            }
        }

        if (!needToGenerate) {
            // Ключи уже готовы
            callback.accept(keysMap.get(name));
        } else {
            generatorPool.submit(() -> {
                try {
                    System.out.printf("[ServerGen Thread-%d] Generating keys for '%s'%n", Thread.currentThread().getId(), name);
                    System.out.flush();
                    KeyBundle bundle = GeneratorRSA.generate(issuerPrivateKey, issuerPublicKey, issuerName, name);
                    System.out.printf("[ServerGen Thread-%d] DONE gen keys for '%s'%n", Thread.currentThread().getId(), name);
                    System.out.flush();

                    synchronized (keysMap) {
                        keysMap.put(name, bundle);
                    }
                    System.out.printf("[Server] Keys for '%s' generated%n", name);
                    System.out.flush();
                    callback.accept(bundle);
                } catch (Exception e) {
                    synchronized (keysMap) {
                        keysMap.remove(name);
                    }
                    System.err.printf("[Server] Failed to generate keys for '%s': %s%n", name, e.getMessage());
                }
            });
        }
    }

    public void shutdown() {
        generatorPool.shutdown();
    }
}
