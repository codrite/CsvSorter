package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class GenerateSortedCsvFileUsingIndexTest {

    @Test
    public void shouldGenerateSortedCSVFile() throws IOException, InterruptedException {
        Path inputCsv = Paths.get("input/smallInput.csv");
        Path indexFile = Files.list(Paths.get("index")).findFirst().orElseThrow(() -> new IllegalArgumentException("No index directory found"));
        Path outputCsvFile = Paths.get("output/out_3_col.csv");
        RecreateSortedCsvFileUsingIndex generateSortedCsvFileUsingIndex = new RecreateSortedCsvFileUsingIndex(inputCsv, indexFile, outputCsvFile);
        generateSortedCsvFileUsingIndex.execute();
    }

}