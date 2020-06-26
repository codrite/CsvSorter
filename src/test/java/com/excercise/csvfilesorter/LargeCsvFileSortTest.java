package com.excercise.csvfilesorter;

import com.excercise.csvfilesorter.step.FirstStepIsToSplitTheCsvFile;
import com.excercise.csvfilesorter.step.SecondStepIsToCreateIndex;
import com.excercise.csvfilesorter.step.ThirdStepIsToRebuildSortedCsvUsingIndex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

/*
    Sort an input csv with 10000 records
 */
public class LargeCsvFileSortTest {

    static Path inputPath = Paths.get("input/largeInput.csv");
    static Path indexPath = Paths.get("target/index");

    static Path outputDir = Paths.get("target/output");
    static Path outputPath = Paths.get("target/output/largeOutput" + ".csv");

    @BeforeAll
    public static void createFolders() throws IOException {
        if(Files.notExists(indexPath))
            Files.createDirectory(indexPath);
        else {
            Set<Path> files = Files.list(indexPath).collect(Collectors.toSet());
            for (Path eachFile : files) {
                Files.delete(eachFile);
            }
        }

        if(Files.notExists(outputDir)) {
            Files.createDirectory(outputDir);
            Files.createFile(outputPath);
        }
    }

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        new FirstStepIsToSplitTheCsvFile(inputPath, 3, 10).execute();
        new SecondStepIsToCreateIndex(1).execute();
        new ThirdStepIsToRebuildSortedCsvUsingIndex(inputPath, indexPath, outputPath).execute();
    }

}
