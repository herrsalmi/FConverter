package fr.ugma;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by Ayyoub on 14/06/2016.
 */
class VCFHandler extends RecursiveAction {

    private static final int MAXSIZE = 100;
    private static int count = 0;
    private static int initialSize = 0;
    private final HashMap<String, ArrayList<String>> hashGenotype = new HashMap<>(100);
    private final AsyncFileWriter writer;
    private int size;
    private int offset;
    private final MemoryTextBuffer buffer;
    private final String chip;

    VCFHandler(int size, int offset, AsyncFileWriter writer, MemoryTextBuffer buffer, String chip) {
        this.size = size;
        this.offset = offset;
        this.writer = writer;
        this.buffer = buffer;
        this.chip = chip;
        if (initialSize == 0)
            initialSize = size - 9;
    }

    @Override
    protected void compute() {
        if (this.size > MAXSIZE) {
            List<VCFHandler> subtasks = new ArrayList<>(createSubtasks());
            for (RecursiveAction subtask : subtasks) {
                subtask.fork();
            }
        } else {
            extractGenotype(buffer, chip);
            incrementCount();
            System.out.print("\rWrote " + count + " out of " + initialSize + " individuals");
        }
    }

    private void extractGenotype(MemoryTextBuffer buffer, String chip) {
        String[] data;
        ArrayList<String> indiv = new ArrayList<>(60);
        if (offset == 0) {
            offset += 9;
            size -= 9;
        }
        for (String line : buffer) {
            if (line.startsWith("##"))
                continue;

            if (line.startsWith("#CHROM")) {
                data = line.split("\t");
                for (int i = offset; i < offset + size; i++) {
                    hashGenotype.put(data[i], new ArrayList<>(buffer.size()));
                    indiv.add(data[i]);
                }
                continue;
            }

            int pos = 0;
            int end;
            for (int i = 0; i < offset; i++) {
                end = line.indexOf('\t', pos);
                pos = end + 1;
            }
            for (int i = offset; i < offset + size; i++) {
                end = line.indexOf('\t', pos);
                end = end != -1 ? end : line.length();
                hashGenotype.get(indiv.get(i - offset)).add(line.substring(pos, end));
                pos = end + 1;
            }
        }

        StringBuilder sb = new StringBuilder();

        hashGenotype.forEach((k, v) -> {
            sb.append(k).append("\t").append(chip).append("\t");
            v.forEach(e -> {
                switch (e) {
                    case "0/0":
                        sb.append("0");
                        break;
                    case "0/1":
                    case "1/0":
                        sb.append("1");
                        break;
                    case "1/1":
                        sb.append("2");
                        break;
                    default:
                        sb.append("5");
                }
            });
            sb.append("\n");
            writer.append(sb.toString());
            sb.setLength(0);
        });
    }

    private List<VCFHandler> createSubtasks() {
        List<VCFHandler> subtasks = new ArrayList<>();
        VCFHandler subtask1 = new VCFHandler(this.size / 2, this.offset, writer, buffer, chip);
        VCFHandler subtask2;
        if (size % 2 == 1)
            subtask2 = new VCFHandler((this.size / 2) + 1, this.offset + this.size / 2, writer, buffer, chip);
        else
            subtask2 = new VCFHandler(this.size / 2, this.offset + this.size / 2, writer, buffer, chip);
        subtasks.add(subtask1);
        subtasks.add(subtask2);

        return subtasks;
    }

    private synchronized void incrementCount() {
        count += size;
    }
}
