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
public class SmallCsvFileSortTest {

    static Path inputPath = Paths.get("input/smallInput.csv");
    static Path stagingPath = Paths.get("staging");

    static Path outputPath = Paths.get("target/smallOutput" + ".csv");
    static Integer INDEX_FIELD = 9;

    @BeforeAll
    public static void createFolders() throws IOException {
        if (Files.notExists(stagingPath))
            Files.createDirectory(stagingPath);
        else {
            Set<Path> files = Files.list(stagingPath).collect(Collectors.toSet());
            for (Path eachFile : files) {
                Files.delete(eachFile);
            }
        }
    }

    @Test
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        new SplitCsvFile(inputPath, INDEX_FIELD, 10).execute();
        log.info("Completed split step in {}", (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        new MergeFiles(INDEX_FIELD, stagingPath.toFile().getPath(), outputPath.toFile().getPath(), 1).execute();
        log.info("Completed building sorted CSV step in {}", (System.currentTimeMillis() - startTime));
    }

}
