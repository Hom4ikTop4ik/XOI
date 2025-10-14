package ru.nsu.martynov;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import static ru.nsu.martynov.SortThread.mySleep;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

//        System.out.print("Input count of threads: ");
//        int num = scanner.nextInt();
        int num = 4;

        System.out.print("Enter -- MyLinkedList, any text -- Collections.SyncList(): ");
        if (scanner.nextLine().isEmpty()) {
            myVer(num);
        }
        syncVer(num);
    }

    public static void myVer(int num) {
        Scanner scanner = new Scanner(System.in);
        MyLinkedList list = new MyLinkedList();

        for (int i = 0; i < num; i++) {
            new SortThread(list, 250, 1000).start();
        }

        while (true) {
            String line = scanner.nextLine();
            if (line.isEmpty()) {
                System.out.println("Current MY list status:");
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

    public static void syncVer(int num) {
        List<String> list = Collections.synchronizedList(new ArrayList<>());
        Scanner scanner = new Scanner(System.in);

        for (int i = 0; i < num; i++) {
            new SortThreadSyncList(list, 250, 1000).start();
        }

        while (true) {
            String line = scanner.nextLine();
            if (line.isEmpty()) {
                System.out.println("Current SYNC list status:");
                for (String s : list) {
                    System.out.println(s);
                    mySleep(50);
                }
            } else {
                for (int i = 0; i < line.length(); i += 80) {
                    int end = Math.min(i + 80, line.length());
                    list.addLast(line.substring(i, end));
                }
            }
        }
    }
}





