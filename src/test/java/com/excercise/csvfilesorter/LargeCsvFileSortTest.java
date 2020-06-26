package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class LargeCsvFileSortTest {

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        new SplitCsvFile("input/largeInput.csv", 3, 10).execute();
        new CreateCSVIndexFile(1).execute();
        new RecreateSortedCsvFileUsingIndex(Paths.get("input/largeInput.csv"), Paths.get("index"), Paths.get("output/largeOutput.csv")).execute();
    }

}
