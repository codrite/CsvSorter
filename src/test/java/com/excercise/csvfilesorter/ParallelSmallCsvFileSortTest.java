package com.excercise.csvfilesorter;

import com.excercise.csvfilesorter.step.SplitCsvFile;
import com.excercise.csvfilesorter.step.MergeFiles;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

/*
    Sort an input csv with 128 records
 */
@Slf4j
public class ParallelSmallCsvFileSortTest {

    static Path inputPath = Paths.get("input/smallInput.csv");
    static Path indexPath = Paths.get("staging");

    static Path outputPath = Paths.get("target/smallOutput" + ".csv");
    static Integer INDEX_FIELD = 9;

    @BeforeAll
    public static void createFolders() throws IOException {
        if (Files.notExists(indexPath))
            Files.createDirectory(indexPath);
        else {
            Set<Path> files = Files.list(indexPath).collect(Collectors.toSet());
            for (Path eachFile : files) {
                Files.delete(eachFile);
            }
        }
    }

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        new SplitCsvFile(inputPath, INDEX_FIELD, 20).execute();
        log.info("Completed split step in {}", (System.currentTimeMillis() - startTime) + " milliseconds ");

        startTime = System.currentTimeMillis();
        new MergeFiles(INDEX_FIELD, indexPath.toFile().getPath(), outputPath.toFile().getPath(), 2).execute();
        log.info("Completed building sorted CSV step in {}", (System.currentTimeMillis() - startTime) + " milliseconds ");
    }

}
