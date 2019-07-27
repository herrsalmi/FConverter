package fr.ugma;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Created by Ayyoub on 12/05/2016.
 */
class Positions implements Comparable {
    private static int CMP = 0;
    private static int CHIP = 1;

    private long pos;
    private String rsID;
    private ArrayList<Integer> chipPos = new ArrayList<>();

    Positions(long pos, String rsID) {
        this.rsID = rsID;
        this.pos = pos;
        for (int i = 0; i < Positions.CHIP; i++) {
            chipPos.add(0);
        }
        chipPos.set(Positions.CHIP - 1, ++CMP);
    }

    static void resetCMP() {
        Positions.CMP = 0;
    }

    static void incrementChipNumber() {
        Positions.CHIP++;
    }

    static int getChipNumber() {
        return Positions.CHIP;
    }

    long getPos() {
        return pos;
    }

    public String getRsID() {
        return rsID;
    }


    private void setChipNPos(int pos) {
        for (int i = chipPos.size(); i < Positions.CHIP; i++) {
            chipPos.add(0);
        }
        chipPos.set(Positions.CHIP - 1, pos);
    }

    int getChipNPos(int chip) {
        try {
            return chipPos.get(chip - 1);
        } catch (IndexOutOfBoundsException e) {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Positions that = (Positions) o;
        return pos == that.pos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pos);
    }

    @Override
    public int compareTo(Object o) {
        if (o == null)
            throw new NullPointerException();
        Positions that = (Positions) o;
        if (this.pos == that.pos) {
            that.setChipNPos(CMP);
            return 0;
        } else if (pos > that.pos)
            return 1;
        else
            return -1;
    }

}
