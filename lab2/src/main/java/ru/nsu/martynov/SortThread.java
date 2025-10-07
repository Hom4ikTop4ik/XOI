package ru.nsu.martynov;

class SortThread extends Thread {
    private final MyLinkedList list;
    private final int delayStep;
    private final int delayCycle;

    public SortThread(MyLinkedList list, int delayStep, int delayCycle) {
        this.list = list;
        this.delayStep = delayStep;
        this.delayCycle = delayCycle;
    }

    public static void mySleep(long ms) {
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            Node current = list.getHead();
            while (current != null) {
                list.trySwapNext(current);
                mySleep(delayStep);
                current = current.next;
            }

            mySleep(delayCycle);
        }
    }
}