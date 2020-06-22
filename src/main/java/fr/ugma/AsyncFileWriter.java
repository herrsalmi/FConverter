package fr.ugma;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Created by Ayyoub on 15/06/2016.
 */
class AsyncFileWriter implements Runnable, IFileWriter, AutoCloseable {
    private final Writer out;
    private final BlockingQueue<Item> queue = new LinkedBlockingQueue<>(10000000);
    private volatile boolean started = false;
    private volatile boolean stopped = false;
    private CountDownLatch lock;

    AsyncFileWriter(File file, boolean compressed) throws IOException {
        lock = new CountDownLatch(1);
        if (compressed) {
            GZIPOutputStream zout = new GZIPOutputStream(new FileOutputStream(file));
            this.out = new BufferedWriter(new OutputStreamWriter(zout));
        } else {
            this.out = new BufferedWriter(new FileWriter(file));
        }
    }

    AsyncFileWriter(File file, CountDownLatch lock, boolean compressed) throws IOException {
        this(file, compressed);
        this.lock = lock;
    }

    public void append(CharSequence seq) {
        if (!started) {
            throw new IllegalStateException("open() call expected before append()");
        }
        try {
            queue.put(new CharSeqItem(seq));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void open() {
        this.started = true;
        new Thread(this).start();
    }

    public boolean isFinished() {
        return queue.isEmpty();
    }

    public void run() {
        Item item;
        while (!stopped || !queue.isEmpty()) {
            try {
                item = queue.poll(100, TimeUnit.MICROSECONDS);
                if (item != null) {
                    try {
                        item.write(out);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            out.close();
            lock.countDown();
        } catch (IOException ignore) {
        }
    }

    public void close() {
        this.stopped = true;
    }

    public CountDownLatch getLock() {
        return lock;
    }

    private interface Item {
        void write(Writer out) throws IOException;
    }

    private static class CharSeqItem implements Item {
        private final CharSequence sequence;

        CharSeqItem(CharSequence sequence) {
            this.sequence = sequence;
        }

        public void write(Writer out) throws IOException {
            out.append(sequence);
        }
    }
}
