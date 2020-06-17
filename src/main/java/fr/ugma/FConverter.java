package fr.ugma;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * Created by Ayyoub on 22/04/2016.
 */
public class FConverter {

    private static final String VERSION = "1.0";
    private static final boolean LARGMEM = false;
    private boolean header = false;
    private mode tMode;
    private final HashMap<String, String> params = new HashMap<>();
    private final LinkedHashMap<String, SortedSet<Positions>> positionSet = new LinkedHashMap<>();
    private ArrayList<String> imputationRes = new ArrayList<>(100);
    private final ArrayList<StringBuilder> buffer = new ArrayList<>();
    private final LinkedHashMap<String, LinkedHashMap<String, String[]>> hashAlleles = new LinkedHashMap<>(100);
    private int cmp = 0;
    private char[] geno;

    private FConverter(String[] args) {
        parseArguments(args);
        entryPoint();
    }

    public static void main(String[] args) {
        new FConverter(args);
    }

    private void parseArguments(String[] args) {
        String errorMsg = "ERROR! : incorrect number of arguments !";
        if (args.length == 0) {
            printHelp();
            System.exit(0);
        }
        switch (args[0]) {
            case "-v":
            case "version":
                System.out.println("FImpute format converter");
                System.out.println("Version: " + VERSION);
                System.exit(0);
                break;
            case "-h":
            case "help":
                printHelp();
                System.exit(0);
                break;
            case "vcf2fimpute":
                if (args.length != 5 && args.length != 4 && args.length != 3) {
                    System.out.println(errorMsg);
                    System.exit(1);
                }
                for (int i = 1; i < args.length; i++) {
                    if (!args[i].equals("-p"))
                        params.put(args[i].split("=")[0], args[i].split("=")[1]);
                    else
                        header = true;
                }
                tMode = mode.VCF2FIMPUTE;
                break;
            case "snpID":
                if (args.length != 3 && args.length != 2) {
                    System.out.println(errorMsg);
                    System.exit(1);
                }
                for (int i = 1; i < args.length; i++) {
                    params.put(args[i].split("=")[0], args[i].split("=")[1]);
                }
                tMode = mode.SNPID;
                break;
            case "fimpute2vcf":
                if (args.length != 6) {
                    System.out.println(errorMsg);
                    System.exit(1);
                }
                for (int i = 1; i < args.length; i++) {
                    params.put(args[i].split("=")[0], args[i].split("=")[1]);
                }
                tMode = mode.FIMPUTE2VCF;
                break;
            default:
                printHelp();
                System.out.println("ERROR! : incorrect option !");
                System.exit(1);
        }
    }

    private void printHelp() {
        System.out.println("FImpute format converter");
        System.out.println("usage : FConverter <vcf2fimpute|snpID|fimpute2vcf> [options]");

        System.out.println("\thelp | -h");
        System.out.println("\t\tprint this help and exit the program");

        System.out.println("\t-v");
        System.out.println("\t\tprint the program version");

        System.out.println("\tvcf2fimpute");
        System.out.println("\t\tvcf=[file]          gzip compressed vcf file ");
        System.out.println("\t\tchip=[integer]      chip number");
        System.out.println("\t\t[-p]                print file header");
        System.out.println("\t\t[nthr=xx]           use xx threads (default=4)");

        System.out.println("\tsnpID");
        System.out.println("\t\thd=[file]           gzip compressed vcf file");
        System.out.println("\t\t[ld=[file]]         gzip compressed vcf file (comma separated if many)");

        System.out.println("\tfimpute2vcf");
        System.out.println("\t\tgen=[file]          imputed genotypes file ");
        System.out.println("\t\tsnp=[file]          snp info file reported by FImpute ");
        System.out.println("\t\tvcf=[file]          gzip compressed vcf file (reference) (comma separated if many)");
        System.out.println("\t\tout=[string]        output file prefix");
        System.out.println("\t\tchip=[integer]      chip number (0 for a VCF with all chips)");
    }

    private void entryPoint() {
        switch (tMode) {
            case VCF2FIMPUTE:
                System.out.println("Converting VCF file to FImpute format ...");
                try (AsyncFileWriter writer = new AsyncFileWriter(new File("genotype_id_c" + params.get("chip") + ".txt"), false)) {
                    if (!Files.exists(Paths.get(params.get("vcf")))) {
                        System.out.println("ERROR! : file <" + params.get("vcf") + "> does not exist");
                        System.exit(2);
                    }
                    int nthr = 4;
                    if (params.containsKey("nthr")) {
                        nthr = Integer.parseInt(params.get("nthr"));
                    }
                    System.out.println("Using " + nthr + " threads");
                    writer.open();
                    if (header)
                        writer.append("ID Chip Call...\n");
                    MemoryTextBuffer textBuffer = new MemoryTextBuffer(params.get("vcf"));
                    textBuffer.load();
                    VCFHandler handler = new VCFHandler(getColumnNunber(params.get("vcf")), 0, writer, textBuffer, params.get("chip"));
                    ForkJoinPool pool = new ForkJoinPool(nthr);

                    pool.invoke(handler);
                    pool.shutdown();
                    pool.awaitTermination(6, TimeUnit.HOURS);
                } catch (IOException | InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                System.out.println("\nDone!");
                break;
            case SNPID:
                System.out.println("Creating SNP ID file ...");
                if (params.containsKey("ld")) {
                    List<String> l = new ArrayList<>();
                    l.add(params.get("hd"));
                    l.addAll(Arrays.asList(params.get("ld").split(",")));
                    makeSnpID(l);
                } else
                    makeSnpID(Collections.singletonList(params.get("hd")));
                System.out.println("Done!");
                break;
            case FIMPUTE2VCF:
                System.out.println("Converting FImpute format to VCF ...");
                String[] paths = params.get("vcf").split(",");
                FImpute2VCF(params.get("gen"), params.get("snp"), paths, params.get("out"), params.get("chip"));
                System.out.println("Done!");
                break;
        }

    }

    private int getColumnNunber(String path) {
        try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
             BufferedReader br = new BufferedReader(new InputStreamReader(gzip))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("##"))
                    continue;

                if (line.startsWith("#CHROM")) {
                    return line.split("\t").length;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private void makeSnpID(List<String> paths) {
        paths.stream().filter(p -> !Files.exists(Paths.get(p))).forEach(p -> {
            System.out.println("ERROR! : file <" + p + "> does not exist");
            System.exit(2);
        });
        for (String p : paths) {
            try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(p));
                 BufferedReader br = new BufferedReader(new InputStreamReader(gzip))) {
                String line;
                String[] data = new String[3];
                int end;
                long lastPos = 0;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;
                    int start = 0;
                    for (int i = 0; i < 3; i++) {
                        end = line.indexOf('\t', start);
                        data[i] = line.substring(start, end);
                        start = end + 1;
                    }
                    positionSet.putIfAbsent(data[0], Collections.synchronizedSortedSet(new TreeSet<>()));
                    if (!positionSet.get(data[0]).isEmpty() && lastPos == Long.parseLong(data[1])) {
                        continue;
                    }
                    synchronized (this) {
                        positionSet.get(data[0]).add(new Positions(Long.parseLong(data[1]), data[2]));
                        lastPos = Long.parseLong(data[1]);
                    }
                }
                Positions.incrementChipNumber();
                Positions.resetCMP();
            } catch (IOException | NumberFormatException e) {
                e.printStackTrace();
            }
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter("snp_info.txt"))) {
            StringBuilder sb = new StringBuilder();
            bw.write("SNP_ID Chr Pos Chip1");
            for (int i = 1; i < paths.size(); i++) {
                bw.write(" Chip" + (i + 1));
            }
            bw.newLine();
            positionSet.forEach((k, v) -> v.forEach(e -> {
                try {
                    sb.append(e.getRsID().equals(".") ? "M" + ++cmp : e.getRsID()).append(" ").append(k).append(" ").append(e.getPos()).append(" ")
                            .append(e.getChipNPos(1));
                    for (int i = 1; i < paths.size(); i++) {
                        sb.append("\t").append(e.getChipNPos(i + 1));
                    }
                    bw.write(sb.toString());
                    bw.newLine();
                    sb.setLength(0);

                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void FImpute2VCF(String path, String snpInfo, String[] vcfPath, String prefix, String chip) {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        if (!Files.exists(Paths.get(snpInfo))) {
            System.out.println("ERROR! : file <" + snpInfo + "> does not exist");
            System.exit(2);
        }
        for (String p : vcfPath) {
            if (!Files.exists(Paths.get(p))) {
                System.out.println("ERROR! : file <" + p + "> does not exist");
                System.exit(2);
            }
        }

        try (BufferedReader br = new BufferedReader(new FileReader(snpInfo))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] data;
                line = line.replaceAll(" +", " ");
                data = line.split(" ");
                hashAlleles.putIfAbsent(data[1], new LinkedHashMap<>(500));
                hashAlleles.get(data[1]).putIfAbsent(data[2], null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String p : vcfPath) {
            try (GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(p));
                 BufferedReader br = new BufferedReader(new InputStreamReader(gzip))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;

                    String[] data = line.split("\t");
                    hashAlleles.get(data[0]).replace(data[1], new String[]{data[2], data[3], data[4]});
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        try (AsyncFileWriter writer = new AsyncFileWriter(new File(prefix + ".vcf.gz"), true)) {
            writer.open();
            writer.append("##fileformat=VCFv4.1\n");
            writer.append("##fileDate=");
            writer.append(LocalDateTime.now().toLocalDate().toString());
            writer.append("\n");
            writer.append("##source=FImpute2.2\n");
            writer.append("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n");
            writer.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT");
            int nbrMarkers = 0;
            int count = 1;

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.replaceAll(" +", " ");
                    if (line.startsWith("ID"))
                        continue;
                    if (!line.split(" ")[1].equals(chip) && !chip.equals("0"))
                        continue;
                    if (nbrMarkers == 0) {
                        nbrMarkers = line.split(" ")[2].length();
                        for (int i = 0; i < nbrMarkers; i++) {
                            buffer.add(new StringBuilder());
                        }
                    }
                    cmp = 0;
                    for (char c : line.split(" ")[2].toCharArray()) {
                        buffer.get(cmp).append(c);
                        cmp++;
                    }
                    System.out.print("\rReading individual " + count++);
                    writer.append("\t" + line.split(" ")[0]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
            if (buffer.isEmpty()) {
                System.out.println("ERROR! : incorrect chip number");
                System.exit(3);
            }
            writer.append("\n");

            /////////////////////// large memory mode
            if (LARGMEM) {
                for (int i = 0; i < nbrMarkers; i++) {
                    buffer.add(new StringBuilder());
                }
                hashAlleles.forEach((chr, m) -> m.forEach((pos, d) -> {
                    buffer.get(cmp).append(chr).append("\t").append(pos).append("\t").append(d[0]).append("\t").append(d[1]).append("\t").append(d[2]).append("\t.\t.\t.\tGT");
                    cmp++;
                }));

                imputationRes.forEach(l -> {
                    geno = l.split(" ")[2].toCharArray();
                    cmp = 0;
                    for (char c : geno) {
                        switch (c) {
                            case '0':
                                buffer.get(cmp).append("\t0/0");
                                break;
                            case '1':
                                buffer.get(cmp).append("\t0/1");
                                break;
                            case '2':
                                buffer.get(cmp).append("\t1/1");
                                break;
                            default:
                                buffer.get(cmp).append("\t./.");
                                break;
                        }
                        cmp++;
                    }
                });

                buffer.forEach(e -> {
                    e.append("\n");
                    writer.append(e.toString());

                });
            }
            /////////////////////// low memory mode
            else {
                StringBuilder sb = new StringBuilder();
                imputationRes = null;
                cmp = 0;

                hashAlleles.forEach((chr, m) -> m.forEach((pos, d) -> {
                    // TODO construct each line in parallel then send them to the writer
                    ////////////////
                    sb.setLength(0);
                    sb.append(chr).append("\t").append(pos).append("\t").append(d[0])
                            .append("\t").append(d[1]).append("\t").append(d[2]).append("\t.\t.\t.\tGT");
                    writer.append(sb.toString());
                    buffer.get(0).chars().forEach(e -> {
                        switch (e) {
                            case '0':
                                writer.append("\t0/0");
                                break;
                            case '1':
                                writer.append("\t0/1");
                                break;
                            case '2':
                                writer.append("\t1/1");
                                break;
                            default:
                                writer.append("\t./.");
                                break;
                        }
                    });
                    writer.append("\n");
                    buffer.remove(0);
                    cmp++;
                    ////////////////
                    System.out.format("\rWriting marker %d", cmp);

                }));

                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private enum mode {
        VCF2FIMPUTE, SNPID, FIMPUTE2VCF
    }
}
