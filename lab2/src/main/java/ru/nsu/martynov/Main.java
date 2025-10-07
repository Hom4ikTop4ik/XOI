package ru.nsu.martynov;

import java.util.Scanner;

import static ru.nsu.martynov.SortThread.mySleep;

public class Main {
    public static void main(String[] args) {
        MyLinkedList list = new MyLinkedList();
        Scanner scanner = new Scanner(System.in);

        for (int i = 0; i < 2; i++) {
            new SortThread(list, 250, 1000).start();
        }

        while (true) {
            String line = scanner.nextLine();
            if (line.isEmpty()) {
                System.out.println("Current list status:");
                for (String s : list) {
                    System.out.println(s);
                    mySleep(50);
                }
            } else {
                // режем на куски по 80 символов
                for (int i = 0; i < line.length(); i += 80) {
                    int end = Math.min(i + 80, line.length());
                    list.addFirst(line.substring(i, end));
                }
            }
        }
    }
}





