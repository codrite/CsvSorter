package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class GenerateSortedCsvFileUsingIndexTest {

    @Disabled("Needs a index file prior run")
    public void shouldGenerateSortedCSVFile() throws IOException {
        Path inputCsv = Paths.get("input/smallInput.csv");
        Path indexFile = Files.list(Paths.get("index")).findFirst().orElseThrow(() -> new IllegalArgumentException("No index directory found"));
        Path outputCsvFile = Paths.get("output/out_3_col.csv");
        ThirdStepIsToRebuildSortedCsvUsingIndex generateSortedCsvFileUsingIndex = new ThirdStepIsToRebuildSortedCsvUsingIndex(inputCsv, indexFile, outputCsvFile);
        generateSortedCsvFileUsingIndex.execute();
    }

}