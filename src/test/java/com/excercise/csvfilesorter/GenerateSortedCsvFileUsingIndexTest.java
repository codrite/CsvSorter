package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class GenerateSortedCsvFileUsingIndexTest {

    @Test
    public void shouldGenerateSortedCSVFile() throws IOException {
        Path inputCsv = Paths.get("input/input.csv");
        Path indexFile = Files.list(Paths.get("index")).findFirst().orElseThrow(() -> new IllegalArgumentException("No index directory found"));
        Path outputCsvFile = Paths.get("output/out.csv");
        GenerateSortedCsvFileUsingIndex generateSortedCsvFileUsingIndex = new GenerateSortedCsvFileUsingIndex(inputCsv, indexFile, outputCsvFile);
        generateSortedCsvFileUsingIndex.execute();
    }

}