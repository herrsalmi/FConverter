package fr.ugma;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.zip.GZIPInputStream;

/**
 * Created by Ayyoub on 22/04/2016.
 */
public class FConverter {

    private static final String VERSION = "0.9";
    private static final boolean DEBUG = true;
    private static final boolean LARGMEM = false;
    private static boolean header = false;
    private mode tMode;
    private HashMap<String, String> params = new HashMap<>();
    private HashMap<String, ArrayList<String>> hashGenotype = new HashMap<>(100);
    private HashMap<String, Integer> hashIndiv = new HashMap<>(100);
    private LinkedHashMap<String, SortedSet<Positions>> positionSet = new LinkedHashMap<>();
    private ArrayList<String> imputationRes = new ArrayList<>(100);
    private ArrayList<StringBuilder> buffer = new ArrayList<>();
    private LinkedHashMap<String, LinkedHashMap<String, String[]>> hashAlleles = new LinkedHashMap<>(100);
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
                if (args.length != 6 && args.length != 5 && args.length != 4 && args.length != 3) {
                    System.out.println("ERROR! : incorrect number of arguments !");
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
                    System.out.println("ERROR! : incorrect number of arguments !");
                    System.exit(1);
                }
                for (int i = 1; i < args.length; i++) {
                    params.put(args[i].split("=")[0], args[i].split("=")[1]);
                }
                tMode = mode.SNPID;
                break;
            case "fimpute2vcf":
                if (args.length != 6) {
                    System.out.println("ERROR! : incorrect number of arguments !");
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
        System.out.println("\t\t[ind=[file]]        text file containing individuals to keep, with one individual by line");

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
                if (DEBUG) {
                    try {
                        if (!Files.exists(Paths.get(params.get("vcf")))) {
                            System.out.println("ERROR! : file <" + params.get("vcf") + "> does not exist");
                            System.exit(2);
                        }
                        int nthr = 4;
                        if (params.containsKey("nthr")) {
                            nthr = Integer.parseInt(params.get("nthr"));
                        }
                        System.out.println("Using " + nthr + " threads");
                        AsyncFileWriter writer = new AsyncFileWriter(new File("genotype_id_c" + params.get("chip") + ".txt"), false);
                        writer.open();
                        if (header)
                            writer.append("ID\tChip\tCall...\n");
                        VCFHandler handler = new VCFHandler(getColumnNunber(params.get("vcf")), 0, writer, params.get("vcf"), params.get("chip"));
                        ForkJoinPool pool = new ForkJoinPool(nthr);

                        pool.invoke(handler);
                        while (pool.getActiveThreadCount() != 0) {
                            Thread.sleep(1000);
                        }
                        writer.close();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("\nDone!");
                    break;
                }
                if (params.containsKey("ind"))
                    extractIndivFromFile(params.get("ind"));
                else
                    extractIndivFromVCF(params.get("vcf"));
                extractGenotype(params.get("vcf"));
                writeGenotype("genotype_id_c" + params.get("chip") + ".txt", params.get("chip"));
                System.out.println("Done!");
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
        try {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
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

    private void extractGenotype(String path) {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        try {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            String line;
            String[] data;
            double lastPos = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("##"))
                    continue;

                if (line.startsWith("#CHROM")) {
                    data = line.split("\t");
                    for (int i = 9; i < data.length; i++) {
                        if (hashIndiv.containsKey(data[i])) {
                            hashIndiv.replace(data[i], i);
                            hashGenotype.put(data[i], new ArrayList<>(10000));
                        }
                    }
                    continue;
                }
                data = line.split("\t");
                if (lastPos == Long.parseLong(data[1]))
                    continue;
                for (String key : hashIndiv.keySet()) {
                    hashGenotype.get(key).add(data[hashIndiv.get(key)]);
                }
                lastPos = Long.parseLong(data[1]);
            }
            br.close();
            gzip.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void extractIndivFromFile(String path) {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            while ((line = br.readLine()) != null) {
                hashIndiv.put(line, null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void extractIndivFromVCF(String path) {
        if (!Files.exists(Paths.get(path))) {
            System.out.println("ERROR! : file <" + path + "> does not exist");
            System.exit(2);
        }
        String line;
        String[] data;
        try {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            while ((line = br.readLine()) != null) {
                if (line.startsWith("##"))
                    continue;
                if (line.startsWith("#CHROM")) {
                    data = line.split("\t");
                    for (int i = 9; i < data.length; i++) {
                        hashIndiv.put(data[i], null);
                    }
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeGenotype(String path, String chip) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
            if (header) {
                bw.write("ID\tChip\tCall...");
                bw.newLine();
            }

            hashGenotype.forEach((k, v) -> {
                try {
                    bw.write(k + "\t" + chip + "\t");
                    v.forEach(e -> {
                        try {
                            switch (e) {
                                case "0/0":
                                    bw.write("0");
                                    break;
                                case "0/1":
                                case "1/0":
                                    bw.write("1");
                                    break;
                                case "1/1":
                                    bw.write("2");
                                    break;
                                default:
                                    bw.write("5");
                            }
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    });
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void makeSnpID(List<String> paths) {
        paths.stream().filter(p -> !Files.exists(Paths.get(p))).forEach(p -> {
            System.out.println("ERROR! : file <" + p + "> does not exist");
            System.exit(2);
        });
        try {
            for (String p : paths) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(p));
                BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
                String line;
                String[] data;
                long lastPos = 0;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;
                    data = line.split("\t");
                    positionSet.putIfAbsent(data[0], Collections.synchronizedSortedSet(new TreeSet<>()));
                    if (positionSet.get(data[0]).size() != 0 && lastPos == Long.parseLong(data[1])) {
                        continue;
                    }
                    synchronized (this) {
                        positionSet.get(data[0]).add(new Positions(Long.parseLong(data[1]), data[2]));
                        lastPos = Long.parseLong(data[1]);
                    }

                }
                Positions.incrementChipNumber();
                Positions.resetCMP();
            }

            BufferedWriter bw = new BufferedWriter(new FileWriter("snp_info.txt"));
            StringBuilder sb = new StringBuilder();
            bw.write("SNP_ID\tChr\tPos\tChip1");
            for (int i = 1; i < paths.size(); i++) {
                bw.write("\tChip" + (i + 1));
            }
            bw.newLine();
            positionSet.forEach((k, v) -> v.forEach(e -> {
                try {
                    sb.append(e.getRsID().equals(".") ? "M" + ++cmp : e.getRsID()).append("\t").append(k).append("\t").append(e.getPos()).append("\t")
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

            bw.close();
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
        try {
            String line;
            String[] data;
            BufferedReader br;

            br = new BufferedReader(new FileReader(snpInfo));
            br.readLine();
            while ((line = br.readLine()) != null) {
                line = line.replaceAll(" +", " ");
                data = line.split(" ");
                hashAlleles.putIfAbsent(data[1], new LinkedHashMap<>(500));
                hashAlleles.get(data[1]).putIfAbsent(data[2], null);
            }

            for (String p : vcfPath) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(p));
                br = new BufferedReader(new InputStreamReader(gzip));
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;

                    data = line.split("\t");
                    //hashAlleles.putIfAbsent(data[0], new LinkedHashMap<>(500));
                    //hashAlleles.get(data[0]).putIfAbsent(data[1], new String[]{data[2], data[3], data[4]});
                    hashAlleles.get(data[0]).replace(data[1], new String[]{data[2], data[3], data[4]});
                }
                br.close();
                gzip.close();
            }


            br = new BufferedReader(new FileReader(path));
            AsyncFileWriter writer = new AsyncFileWriter(new File(prefix + ".vcf.gz"), true);
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
                //imputationRes.add(line);
                writer.append("\t" + line.split(" ")[0]);
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
                System.gc();
                cmp = 0;
                hashAlleles.forEach((chr, m) -> m.forEach((pos, d) -> {
                    sb.setLength(0);
                    sb.append(chr).append("\t").append(pos).append("\t").append(d[0]).append("\t").append(d[1]).append("\t").append(d[2]).append("\t.\t.\t.\tGT");
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
                    System.out.format("\rWriting marker %d", cmp);

                }));

                System.out.println();
                //long endTime = System.nanoTime();
                //System.out.println((endTime - startTime)/1000000000 + "s");
//                hashAlleles.forEach((chr, m) -> m.forEach((pos, d) -> {
//                    sb.setLength(0);
//                    sb.append(chr).append("\t").append(pos).append("\t").append(d[0]).append("\t").append(d[1]).append("\t").append(d[2]).append("\t.\t.\t.\tGT");
//                        writer.append(sb.toString());
//                        imputationRes.forEach(l -> {
//                            //geno = l.split(" ")[2].toCharArray();
//                            switch (l.split(" ")[2].charAt(cmp)) {
//                                case '0':
//                                    writer.append("\t0/0");
//                                    break;
//                                case '1':
//                                    writer.append("\t0/1");
//                                    break;
//                                case '2':
//                                    writer.append("\t1/1");
//                                    break;
//                                default:
//                                    writer.append("\t./.");
//                                    break;
//                            }
//                        });
//                        writer.append("\n");
//                    cmp++;
//                }));
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private enum mode {
        VCF2FIMPUTE, SNPID, FIMPUTE2VCF
    }
}
