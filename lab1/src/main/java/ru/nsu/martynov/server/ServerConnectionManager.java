package ru.nsu.martynov.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.cert.CertificateEncodingException;
import java.util.Iterator;

public class ServerConnectionManager {
    private final ServerGen keyService;
    private final int port;
    private volatile boolean running = true;
    private Selector selector;
    private ServerSocketChannel serverChannel;

    private final long startTime = System.currentTimeMillis();

    public ServerConnectionManager(ServerGen keyService, int port) {
        this.keyService = keyService;
        this.port = port;
    }

    private void log(String message) {
        long seconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("[Server %d] %s%n", seconds, message);
        System.out.flush();
    }

    public void start() throws IOException, CertificateEncodingException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        log("ConnectionManager started on port " + port);

        while (running) {
            selector.select(500); // таймаут для проверки running
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                try {
                    if (!key.isValid()) continue;

                    if (key.isAcceptable()) {
                        acceptClient((ServerSocketChannel) key.channel());
                    } else if (key.isReadable()) {
                        readClient(key);
                    } else if (key.isWritable()) {
                        writeClient(key);
                    }
                } catch (Exception e) {
                    log("Error handling key: " + e);
                    try { key.channel().close(); } catch (IOException ignored) {}
                }
            }
        }

        try { selector.close(); } catch (Exception ignored) {}
        try { serverChannel.close(); } catch (Exception ignored) {}
        log("ConnectionManager stopped");
    }

    public void stop() {
        running = false;
        if (selector != null) selector.wakeup();
    }

    private void acceptClient(ServerSocketChannel serverChannel) throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(256));
        log("Accepted new client: " + clientChannel.getRemoteAddress());
    }

    private void readClient(SelectionKey key) {
        try {
            SocketChannel clientChannel = (SocketChannel) key.channel();
            ByteBuffer buffer = (ByteBuffer) key.attachment();
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead == -1) {
                clientChannel.close();
                return;
            }

            // Проверяем нулевой байт (конец имени)
            for (int i = buffer.position() - bytesRead; i < buffer.position(); i++) {
                if (buffer.get(i) == 0) {
                    buffer.flip();
                    byte[] nameBytes = new byte[buffer.limit() - 1];
                    buffer.get(nameBytes);
                    buffer.get(); // пропускаем нулевой байт
                    String name = new String(nameBytes, "US-ASCII");
                    log("Got client name: " + name);
                    log("Start generate...");

                    // Асинхронная генерация ключей
                    keyService.getOrCreateKeysAsync(name, bundle -> {
                        try {
                            byte[] priv = bundle.getPrivateKey().getEncoded();
                            byte[] cert = bundle.getCertificate().getEncoded();
                            ByteBuffer out = ByteBuffer.allocate(4 + priv.length + 4 + cert.length);
                            out.putInt(priv.length);
                            out.put(priv);
                            out.putInt(cert.length);
                            out.put(cert);
                            out.flip();

                            synchronized (selector) {
                                key.attach(out);
                                key.interestOps(SelectionKey.OP_WRITE);
                                selector.wakeup();
                            }
                        } catch (Exception e) {
                            log("Error preparing buffer for " + name + ": " + e);
                        }
                    });

                    return; // основной поток селектора не ждёт ключей
                }
            }

        } catch (Exception e) {
            log("Error in readClient: " + e);
            try { key.channel().close(); } catch (IOException ignored) {}
        }
    }

    private void writeClient(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        clientChannel.write(buffer);
        if (!buffer.hasRemaining()) {
            clientChannel.close();
            log("Sent keys and closed connection");
        }
    }
}
