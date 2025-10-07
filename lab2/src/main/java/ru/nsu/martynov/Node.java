package ru.nsu.martynov;

import java.util.concurrent.locks.ReentrantLock;

class Node {
    String value;
    Node next;
    Node prev;
    final ReentrantLock lock = new ReentrantLock();

    Node(String value) {
        this.value = value;
    }
}
