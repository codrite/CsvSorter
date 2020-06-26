package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/*
 * Use the index file and rebuild the CSV file, sorted this time
 * on the index field
 */
@Slf4j
public class RecreateSortedCsvFileUsingIndex {

    final Path inputCsvFile;
    final Path indexFile;
    final Path sortedCsvFile;

    public RecreateSortedCsvFileUsingIndex(Path inputCsvFile, Path indexFile, Path sortedCsvFile) {
        this.inputCsvFile = inputCsvFile;
        this.indexFile = indexFile;
        this.sortedCsvFile = sortedCsvFile;
    }

    public void execute() throws IOException {
        List<Path> files = Files.list(indexFile).collect(Collectors.toList());
        try(BufferedWriter bufferedWriter = Files.newBufferedWriter(sortedCsvFile)) {
            Files.lines(files.get(0)).forEach(line -> writeToFile(bufferedWriter, line));
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
