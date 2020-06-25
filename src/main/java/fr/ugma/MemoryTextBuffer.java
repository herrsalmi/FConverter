package fr.ugma;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

public class MemoryTextBuffer implements Iterable<String> {
    private final List<String> data;
    private final String path;

    public MemoryTextBuffer(String path) {
        this.path = path;
        data = new CopyOnWriteArrayList<>();
    }

    public void load() {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
             BufferedReader br = new BufferedReader(new InputStreamReader(gzip))) {
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int size() {
        return data.size();
    }

    @Override
    public Iterator<String> iterator() {
        return data.iterator();
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        data.forEach(action);
    }
}
