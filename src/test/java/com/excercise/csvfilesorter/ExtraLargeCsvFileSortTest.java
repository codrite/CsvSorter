package com.excercise.csvfilesorter;

import com.excercise.csvfilesorter.step.SplitCsvFile;
import com.excercise.csvfilesorter.step.MergeFiles;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

/*
    Sort an input csv with 1.5 million records
 */
@Slf4j
public class ExtraLargeCsvFileSortTest {

    static Path inputPath = Paths.get("input/extraLargeInput.csv");
    static Path indexPath = Paths.get("staging");

    static Path outputPath = Paths.get("target/extraLargeOutput" + ".csv");

    static Integer INDEX_FIELD = 3;

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

    @Disabled("Github not allowing me to add +100mb csv file")
    public void shouldSortACsvFile() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        new SplitCsvFile(inputPath, INDEX_FIELD, 100000).execute();
        log.info("Completed split step in {}", (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        new MergeFiles(INDEX_FIELD, indexPath.toFile().getPath(), outputPath.toFile().getPath(), 1).execute();
        log.info("Completed building sorted CSV step in {}", (System.currentTimeMillis() - startTime));
    }

}
