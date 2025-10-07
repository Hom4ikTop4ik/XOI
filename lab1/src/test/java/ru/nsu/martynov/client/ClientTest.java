package ru.nsu.martynov.client;

import org.junit.jupiter.api.*;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ClientTest {

    private static final int TEST_PORT = 54321;
    private static ExecutorService serverExecutor;

    @BeforeAll
    static void setupServer() throws IOException {
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try (ServerSocketChannel server = ServerSocketChannel.open()) {
                server.bind(new InetSocketAddress(TEST_PORT));
                SocketChannel client = server.accept(); // один клиент
                ByteBuffer buf = ByteBuffer.allocate(4);
                // Эмуляция: отправим ключ и сертификат
                byte[] fakeKey = "FAKE_KEY".getBytes();
                byte[] fakeCert = "FAKE_CERT".getBytes();

                // Отправка ключа
                buf.putInt(fakeKey.length);
                buf.flip();
                client.write(buf);
                buf.clear();
                client.write(ByteBuffer.wrap(fakeKey));

                // Отправка сертификата
                buf.putInt(fakeCert.length);
                buf.flip();
                client.write(buf);
                client.write(ByteBuffer.wrap(fakeCert));

                client.shutdownOutput();
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @AfterAll
    static void shutdownServer() {
        serverExecutor.shutdownNow();
    }

    @Test
    void testClientSavesFiles() throws Exception {
        String name = "alice";
        Client client = new Client("localhost", TEST_PORT, name);
        client.setDelaySeconds(0);
        client.setExitBeforeRead(false);

        // Чистим папку перед тестом
        Path dir = Paths.get("clientCerts");
        if (Files.exists(dir)) {
            Files.walk(dir).map(Path::toFile).forEach(File::delete);
        }

        client.start();

        // Проверка, что создана папка
        assertTrue(Files.exists(dir) && Files.isDirectory(dir));

        // Проверка, что файлы созданы
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, name + "_*.key")) {
            boolean keyExists = ds.iterator().hasNext();
            assertTrue(keyExists, "Private key file should exist");
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, name + "_*.crt")) {
            boolean certExists = ds.iterator().hasNext();
            assertTrue(certExists, "Certificate file should exist");
        }
    }
}
