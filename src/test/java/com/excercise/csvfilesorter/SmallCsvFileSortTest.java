package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class SmallCsvFileSortTest {

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        new SplitCsvFile("input/smallInput.csv", 9, 10).execute();
        new CreateCSVIndexFile(1).execute();
        new RecreateSortedCsvFileUsingIndex(Paths.get("input/smallInput.csv"), Paths.get("index"), Paths.get("output/smallOutput.csv")).execute();
    }

}
