package ru.nsu.martynov;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Pavuk {
    private final HttpClient client;
    private final String baseUrl;
    private final ExecutorService executor;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private final ObjectMapper mapper = new ObjectMapper(); // Jackson

    private final int MAX_TIMEOUT = 20; // в условии обещали от 0 до 12

    // автосортировка включена в messages
    private final ConcurrentSkipListSet<String> messages = new ConcurrentSkipListSet<>();
    private final Set<String> visited = ConcurrentHashMap.newKeySet();

    public Pavuk(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void runPavuk() throws InterruptedException {
        queue.add("/");

        int workers = 50;
        CountDownLatch latch = new CountDownLatch(workers);

        for (int i = 0; i < workers; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        String path = queue.poll(MAX_TIMEOUT, TimeUnit.SECONDS);
                        if (path == null) break; // очередь пустая -> завершаем поток
                        processPath(path);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(); // дождаться завершения всех workers
        executor.shutdownNow();

        for (String msg : messages) {
            System.out.println(msg);
        }
    }

    private void processPath(String path) {
        if (path == null || !visited.add(path)) return;

        // чтобы корень смотреть нормально
        String fullPath = path.startsWith("/") ? path : "/" + path;
        String url = baseUrl + fullPath;

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(MAX_TIMEOUT))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = client.send(req, BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                System.err.println("Non-200 for " + fullPath + ": " + resp.statusCode());
                return;
            }

            ServerResponse sr = mapper.readValue(resp.body(), ServerResponse.class);
            if (sr.getMessage() != null) messages.add(sr.getMessage());

            List<String> succ = sr.getSuccessors();
            if (succ != null) {
                for (String s : succ) {
                    if (s == null || s.isEmpty()) continue;
                    if (!visited.contains(s)) {
                        //String childPath = fullPath + "/" + s;
                        String childPath = "/" + s;
                        queue.add(childPath);
                    }
                }
            }
            visited.add(path);

        } catch (Exception e) {
            System.err.println("Error fetching " + fullPath + ", put in queue for retrying later");
            e.printStackTrace();
            queue.add(path);
            visited.remove(path);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        int port = 8080;

        if (args.length >= 1) {
            try { port = Integer.parseInt(args[0]); } catch (Exception ignored) {}
        }
        if (args.length >= 2) {
            host = args[1];
        }

        Pavuk pavuk = new Pavuk(host, port);
        pavuk.runPavuk();
    }
}
