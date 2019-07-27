package fr.ugma;

/**
 * Created by Ayyoub on 15/06/2016.
 */
public interface IFileWriter {
    void append(CharSequence seq);

    void close();
}
