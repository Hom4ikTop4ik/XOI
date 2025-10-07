package ru.nsu.martynov.client;

import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Client {
    private final String host;
    private final int port;
    private final String name;
    private int delaySeconds = 0;
    private boolean exitBeforeRead = false;

    public Client(String host, int port, String name) {
        this.host = host;
        this.port = port;
        this.name = name;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -jar client.jar <host> <port> <name> [--delay <seconds>] [--exit-before-read]");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String name = args[2];

        int delay = 0;
        boolean exitBeforeRead = false;

        for (int i = 3; i < args.length; i++) {
            if ("--delay".equals(args[i]) && i + 1 < args.length) {
                delay = Integer.parseInt(args[++i]);
            } else if ("--exit-before-read".equals(args[i])) {
                exitBeforeRead = true;
            }
        }

        Client client = new Client(host, port, name);
        client.setDelaySeconds(delay);
        client.setExitBeforeRead(exitBeforeRead);
        client.start();
    }

    public void setDelaySeconds(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public void setExitBeforeRead(boolean exitBeforeRead) {
        this.exitBeforeRead = exitBeforeRead;
    }

    public void start() throws Exception {
        System.out.printf("[Client %s] connecting to %s:%d%n", name, host, port);
        System.out.flush();
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(host, port));
            // Отправляем имя (ASCII + 0)
            ByteBuffer out = ByteBuffer.allocate(name.length() + 1);
            System.out.printf("[Client %s] Connected to server%n", name);
            System.out.flush();

            out.put(name.getBytes("US-ASCII"));
            out.put((byte) 0);
            out.flip();
            channel.write(out);
            System.out.printf("[Client %s] Sent name to server%n", name);
            System.out.flush();


            if (delaySeconds > 0) {
                System.out.printf("[Client %s] 1_delaying %d seconds before reading response%n", name, delaySeconds);
                System.out.flush();
                Thread.sleep(delaySeconds * 1000L);
            }
            if (exitBeforeRead) {
                System.out.printf("[Client %s] exiting before reading response%n", name);
                System.out.flush();
                return;
            }

            // Читаем ответ: [4 байта длина][ключ][4 байта длина][сертификат]
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            readFully(channel, lenBuf);
            lenBuf.flip();
            if (delaySeconds > 0) {
                System.out.printf("[Client %s] 2_delaying %d seconds before reading response%n", name, delaySeconds);
                System.out.flush();
                Thread.sleep(delaySeconds * 1000L);
            }
            int keyLen = lenBuf.getInt();
            ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
            readFully(channel, keyBuf);

            if (delaySeconds > 0) {
                System.out.printf("[Client %s] 3_delaying %d seconds before reading response%n", name, delaySeconds);
                System.out.flush();
                Thread.sleep(delaySeconds * 1000L);
            }

            lenBuf.clear();
            readFully(channel, lenBuf);
            lenBuf.flip();
            if (delaySeconds > 0) {
                System.out.printf("[Client %s] 4_delaying %d seconds before reading response%n", name, delaySeconds);
                System.out.flush();
                Thread.sleep(delaySeconds * 1000L);
            }
            int certLen = lenBuf.getInt();
            ByteBuffer certBuf = ByteBuffer.allocate(certLen);
            readFully(channel, certBuf);

            // Сохраняем в файлы
            String clientFolder = "clientCerts/";
            Path dir = Paths.get(clientFolder);
            if (!Files.exists(dir)) {
                // если папка не существует — создаём
                Files.createDirectories(dir);
            }
            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            String keyFile = clientFolder + name + "_" + timestamp + ".key";
            String certFile = clientFolder + name + "_" + timestamp + ".crt";

            try (FileOutputStream kf = new FileOutputStream(keyFile)) {
                kf.write(keyBuf.array());
            }
            try (FileOutputStream cf = new FileOutputStream(certFile)) {
                cf.write(certBuf.array());
            }
            System.out.printf("[Client %s] saved private key to %s, certificate to %s%n", name, keyFile, certFile);
            System.out.flush();
        }
    }

    private void readFully(SocketChannel channel, ByteBuffer buf) throws Exception {
        while (buf.hasRemaining()) {
            int r = channel.read(buf);
            if (r == -1) throw new RuntimeException("Connection closed by server");
        }
    }
}
