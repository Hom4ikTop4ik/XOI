package ru.nsu.martynov.server;

public class ServerMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java -jar <jarfile> <port> <threads> [--issuer <issuerName>] [--key <privateKeyPath>] [--cert <certificatePath>]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);

        // Значения по умолчанию
        String issuerName = "DefaultCA";
        String privateKeyPath = "ca_private.pem";
        String certificatePath = "ca_cert.pem";

        // Разбор флагов
        for (int i = 2; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--issuer":
                    issuerName = args[++i];
                    break;
                case "--key":
                    privateKeyPath = args[++i];
                    break;
                case "--cert":
                    certificatePath = args[++i];
                    break;
            }
        }

        try {
            KeyGenServer server = new KeyGenServer(port, threads, issuerName, privateKeyPath, certificatePath);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}