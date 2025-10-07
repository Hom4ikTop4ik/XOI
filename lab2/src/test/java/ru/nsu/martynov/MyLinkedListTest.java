package ru.nsu.martynov;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MyLinkedListTest {

    @Test
    void testSortingWithBackgroundThreads() throws InterruptedException {
        MyLinkedList list = new MyLinkedList();

        for (int i = 0; i < 2; i++) {
            new SortThread(list, 250, 1000).start();
        }

        // [1..7] without 5
        list.addFirst("1");
        list.addFirst("2");
        list.addFirst("3");
        list.addFirst("4");
        list.addFirst("6");
        list.addFirst("7");

        // __[i hope that 20 seconds will be enough for this 🙏]__
        Thread.sleep(20_000);
        assertSorted(list, "List isn't sorted after initial insert.");

        list.addFirst("5");

        // Wait 20 secs more
        Thread.sleep(20_000);
        assertSorted(list, "List isn't sorted after inserting 5.");
    }

    private void assertSorted(MyLinkedList list, String msg) {
        Node current = list.getHead();
        while (current != null && current.next != null) {
            String a = current.value;
            String b = current.next.value;
            assertTrue(a.compareTo(b) <= 0,
                    msg + " Found " + a + " > " + b);
            current = current.next;
        }
    }
}
