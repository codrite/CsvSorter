package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Slf4j
public class Merge2Files {

    public void merge(Path first, Path second) throws IOException {
        final String filename = "Index" + System.currentTimeMillis();

        Stream<String> firstFile = Files.newBufferedReader(first).lines();
        Stream<String> secondFile = Files.newBufferedReader(second).lines();

        firstFile.forEach(fRow -> secondFile.forEach(sRow -> compareAndWriteToFile(filename, fRow, sRow)));
    }

    private void compareAndWriteToFile(String filename, String lineOne, String lineTwo) {
        try {
            if (lineOne.compareTo(lineTwo) > 0)
                writeToFile(filename, lineTwo);
            else
                writeToFile(filename, lineOne);
        } catch (IOException ioException) {
            log.error(ioException.getMessage());
        }
    }

    private void writeToFile(String filename, String line) throws IOException {
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(filename))) {
            bufferedWriter.write(line);
        }
    }

}
