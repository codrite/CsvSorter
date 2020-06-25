package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class GenerateSortedCsvFileUsingIndex {

    final Path inputCsvFile;
    final Path indexFile;
    final Path sortedCsvFile;

    public GenerateSortedCsvFileUsingIndex(Path inputCsvFile, Path indexFile, Path sortedCsvFile) {
        this.inputCsvFile = inputCsvFile;
        this.indexFile = indexFile;
        this.sortedCsvFile = sortedCsvFile;
    }

    public void execute() throws IOException {
        try(BufferedWriter bufferedWriter = Files.newBufferedWriter(sortedCsvFile)) {
            Files.lines(indexFile).forEach(line -> writeToFile(bufferedWriter, line));
        }
    }

    void writeToFile(BufferedWriter bufferedWriter, String line) {
        try {
            String lineString = Files.lines(inputCsvFile).skip(Long.parseLong(line.split(",")[1]) - 1).findFirst().orElseThrow(null);
            bufferedWriter.write(lineString);
            bufferedWriter.newLine();
        } catch (IOException ioException) {
            log.error(ioException.getMessage());
        }
    }

}
