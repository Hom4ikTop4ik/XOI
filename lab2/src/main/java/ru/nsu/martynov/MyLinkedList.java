package ru.nsu.martynov;

import java.util.Iterator;
import java.util.NoSuchElementException;

class MyLinkedList implements Iterable<String> {
    private Node head = null;

    public void addFirst(String value) {
        Node node = new Node(value);
        if (head == null) {
            head = node;
            return;
        }

        head.lock.lock();
        Node oldHead = head;
        try {
            node.next = head;
            head.prev = node;
            head = node;
        } finally {
            oldHead.lock.unlock();
        }
    }

    public Node getHead() {
        return head;
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<>() {
            Node current = head;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public String next() {
                if (current == null) throw new NoSuchElementException();

                current.lock.lock();
                Node oldCurrent = current;
                try {
                    String val = current.value;
                    current = current.next;  // переходим к следующему узлу
                    return val;
                } finally {
                    oldCurrent.lock.unlock();  // безопасно разблокируем текущий узел
                }
            }
        };
    }

    public boolean trySwapNext(Node first) {
        if (first == null) {
            return false;
        }

        // current sorterThread
        //         \/
        // fPrev first second sNext
        //
        //
        // другие sorters могут указывать на second, sNext, sNext.next
        // если они и поменяют местами наши first и second, либо second и sNext, то это всё равно не страшно
        // потому что мы перевыберем first, second, sNext потом, когда нам отдадут последующие lock.
        // если другому сортировщику передать second или последующие, то first останется на месте что нам на руку
        // если передать другому сортировщику first, он поменяет местами second и first, тогда мы этого делать не будем
        // либо мы будем менять местами уже first и sNext, если вдруг второй сортировщик быстренько проделает несколько этапов (ну а вдруг)
        //
        // Коротко: мы делаем шаг назад на fPrev, затем переузнаём first, second и sNext на основе fPrev; потом сортируем

        first.lock.lock();
        Node fPrev = first.prev;
        first.lock.unlock();

        if (fPrev != null) {
            fPrev.lock.lock();
        }

        try {
            if (fPrev != null) {
                first = fPrev.next;
            }

            first.lock.lock();
            try {
                Node second = first.next;
                if (second == null) {
                    return false;
                }

                second.lock.lock();
                try {
                    if (first.value.compareTo(second.value) > 0) {
                        Node sNext = second.next;
                        if (sNext != null) sNext.lock.lock();

                        try {
                            if (fPrev != null) fPrev.next = second;
                            second.prev = fPrev;

                            second.next = first;
                            first.prev = second;

                            first.next = sNext;
                            if (sNext != null) sNext.prev = first;

                            if (head == first) head = second;
                        } finally {
                            if (sNext != null) sNext.lock.unlock();
                        }
                        return true;
                    }
                } finally {
                    second.lock.unlock();
                }
            } finally {
                first.lock.unlock();
            }
        }
        finally {
            if (fPrev != null) {
                fPrev.lock.unlock();
            }
        }

        return false;
    }
}