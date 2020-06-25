package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class CsvFileSortTest {

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        new SplitCsvFile("input/input.csv", 9, 2).execute();
        Thread.sleep(15000);

        GenerateCSVIndexFile generateCSVIndexFile = new GenerateCSVIndexFile(1);
        generateCSVIndexFile.execute();

        while(!generateCSVIndexFile.isComplete()){Thread.sleep(5000);}

        new GenerateSortedCsvFileUsingIndex(Paths.get("input/input.csv"), Paths.get("index"), Paths.get("output/out.csv"));
    }

}
