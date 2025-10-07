package ru.nsu.martynov;

import ru.nsu.martynov.server.KeyGenServer;
import ru.nsu.martynov.client.Client;

public class Main {
    public static void main(String[] args) throws Exception {
        KeyGenServer server = new KeyGenServer(12345, 4, "TestCA", "ca_private.pem", "ca_cert.pem");
        // Запуск сервера в отдельном потоке
        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Дать серверу время стартовать
        Thread.sleep(2000);

        // Запуск нескольких клиентов
        Thread client1 = new Thread(() -> runClient("localhost", 12345, "alice", 10, false));
        Thread client2 = new Thread(() -> runClient("localhost", 12345, "bob", 20, false));
        Thread client3 = new Thread(() -> runClient("localhost", 12345, "charlie", 1, true));

        client1.start();
        client2.start();
        client3.start();

        client1.join();
        client2.join();
        client3.join();

        Thread.sleep(20000);

        Thread client4 = new Thread(() -> runClient("localhost", 12345, "alice", 0, false));
        client4.start();
        client4.join();

        server.stop();
        serverThread.join();
    }

    private static void runClient(String host, int port, String name, int delay, boolean exitBeforeRead) {
        try {
            Client client = new Client(host, port, name);
            client.setDelaySeconds(delay);
            client.setExitBeforeRead(exitBeforeRead);
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}