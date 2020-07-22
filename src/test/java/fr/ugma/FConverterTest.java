package fr.ugma;


import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FConverterTest {

    @Test
    @Order(1)
    void vcf2fimpute() {
        String[] args = {"vcf2fimpute", "vcf=54k.reduced.vcf.gz", "chip=1", "-p"};
        assertDoesNotThrow(() -> FConverter.main(args));
    }

    @Test
    @Order(2)
    void snpID() {
        String[] args = {"snpID", "hd=54k.reduced.vcf.gz"};
        assertDoesNotThrow(() -> FConverter.main(args));
    }

    @Test
    @Order(3)
    void fimpute2vcf() {
        String[] args = {"fimpute2vcf", "gen=genotype_id_c1.txt", "snp=snp_info.txt", "vcf=54k.reduced.vcf.gz",
                "out=demo", "chip=0"};
        assertDoesNotThrow(() -> FConverter.main(args));
    }
}