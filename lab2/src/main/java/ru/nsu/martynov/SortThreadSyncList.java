package ru.nsu.martynov;

import java.util.Collections;
import java.util.List;

class SortThreadSyncList extends Thread {
    private final List<String> list;
    private final int delayStep;
    private final int delayCycle;

    public SortThreadSyncList(List<String> list, int delayStep, int delayCycle) {
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
            for (int i = 0; i < list.size() - 1; i++) {
                synchronized (list) {
                    String a = list.get(i);
                    String b = list.get(i + 1);
                    if (a.compareTo(b) > 0) {
                        Collections.swap(list, i, i + 1);
                    }
                }
                mySleep(delayStep);
            }
            mySleep(delayCycle);
        }
    }
}

