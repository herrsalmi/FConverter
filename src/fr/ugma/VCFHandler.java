package fr.ugma;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RecursiveAction;
import java.util.zip.GZIPInputStream;

/**
 * Created by Ayyoub on 14/06/2016.
 */
class VCFHandler extends RecursiveAction {

    private static final int MAXSIZE = 100;
    private static int count = 0;
    private static int initialSize = 0;
    private HashMap<String, ArrayList<String>> hashGenotype = new HashMap<>(100);
    private AsyncFileWriter writer;
    private int size;
    private int offset;
    private String path;
    private String chip;

    VCFHandler(int size, int offset, AsyncFileWriter writer, String path, String chip) {
        this.size = size;
        this.offset = offset;
        this.writer = writer;
        this.path = path;
        this.chip = chip;
        if (initialSize == 0)
            initialSize = size - 9;
    }

    @Override
    protected void compute() {
        if (this.size > MAXSIZE) {
            //System.out.println("Splitting size: " + this.size + " [Offset] " + this.offset);
            List<VCFHandler> subtasks = new ArrayList<>(createSubtasks());
            for (RecursiveAction subtask : subtasks) {
                subtask.fork();
            }
        } else {
            extractGenotype(path, chip);
            while (!writer.isFinished()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            incrementCount();
            System.out.print("\rWrote " + count + " out of " + initialSize + " individuals");
        }
    }

    private void extractGenotype(String path, String chip) {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        try {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            String line;
            String[] data;
            ArrayList<String> indiv = new ArrayList<>(100);
            if (offset == 0) {
                offset += 9;
                size -= 9;
            }
            while ((line = br.readLine()) != null) {
                if (line.startsWith("##"))
                    continue;

                if (line.startsWith("#CHROM")) {
                    data = line.split("\t");
                    for (int i = offset; i < offset + size; i++) {
                        hashGenotype.put(data[i], new ArrayList<>(10000));
                        indiv.add(data[i]);
                    }
                    continue;
                }

                data = line.split("\t");

                for (int i = offset; i < offset + size; i++) {
                    hashGenotype.get(indiv.get(i - offset)).add(data[i]);
                }
            }
            br.close();
            gzip.close();

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

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<VCFHandler> createSubtasks() {
        List<VCFHandler> subtasks = new ArrayList<>();
        VCFHandler subtask1 = new VCFHandler(this.size / 2, this.offset, writer, path, chip);
        VCFHandler subtask2;
        if (size % 2 == 1)
            subtask2 = new VCFHandler((this.size / 2) + 1, this.offset + this.size / 2, writer, path, chip);
        else
            subtask2 = new VCFHandler(this.size / 2, this.offset + this.size / 2, writer, path, chip);
        subtasks.add(subtask1);
        subtasks.add(subtask2);

        return subtasks;
    }

    private synchronized void incrementCount() {
        count += size;
    }
}
