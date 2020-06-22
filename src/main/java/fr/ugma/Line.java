package fr.ugma;

import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Line {
    private final String chr;
    private final int pos;
    private final String id;
    private final String ref;
    private final String alt;
    private final IntStream genotypes;

    private final IntFunction<String> mapper;

    public Line(String chr, int pos, String id, String ref, String alt, IntStream genotypes) {
        this.chr = chr;
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.genotypes = genotypes;

        mapper = e -> {
            switch (e) {
                case '0':
                    return "\t0/0";
                case '1':
                    return "\t0/1";
                case '2':
                    return "\t1/1";
                default:
                    return "\t./.";
            }
        };
    }

    @Override
    public String toString() {
        return chr + '\t' + pos + '\t' + id + '\t' + ref + '\t' + alt + "\t.\t.\t.\tGT"
                + genotypes.mapToObj(mapper).collect(Collectors.joining()) + "\n";
    }
}
