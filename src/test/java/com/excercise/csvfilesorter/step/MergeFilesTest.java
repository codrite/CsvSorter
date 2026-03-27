package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MergeFilesTest {

    @TempDir
    Path tempDir;

    Path stagingDir;
    Path outputFile;

    @BeforeEach
    void setUp() throws IOException {
        stagingDir = tempDir.resolve("staging");
        Files.createDirectories(stagingDir);
        outputFile = tempDir.resolve("output.csv");
    }

    @Test
    void execute_shouldMergeTwoFilesIntoSortedOutput() throws IOException, InterruptedException {
        Files.write(stagingDir.resolve("TEMP_0.csv"), Arrays.asList("apple,1", "cherry,3"));
        Files.write(stagingDir.resolve("TEMP_1.csv"), Arrays.asList("banana,2", "date,4"));

        MergeFiles mergeFiles = new MergeFiles(1, stagingDir.toString(), outputFile.toString(), 1);
        mergeFiles.execute();

        assertTrue(Files.exists(outputFile), "Output file should exist");
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(4, lines.size());
        assertEquals("apple,1", lines.get(0));
        assertEquals("banana,2", lines.get(1));
        assertEquals("cherry,3", lines.get(2));
        assertEquals("date,4", lines.get(3));
    }

    @Test
    void execute_shouldHandleSingleInputFile() throws IOException, InterruptedException {
        Files.write(stagingDir.resolve("TEMP_0.csv"), Arrays.asList("apple,1", "banana,2"));

        MergeFiles mergeFiles = new MergeFiles(1, stagingDir.toString(), outputFile.toString(), 1);
        mergeFiles.execute();

        assertTrue(Files.exists(outputFile));
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(2, lines.size());
        assertEquals("apple,1", lines.get(0));
        assertEquals("banana,2", lines.get(1));
    }

    @Test
    void execute_shouldMergeThreeFiles() throws IOException, InterruptedException {
        // Odd number of files: two get merged, one waits for next round
        Files.write(stagingDir.resolve("TEMP_0.csv"), Arrays.asList("apple,1", "date,4"));
        Files.write(stagingDir.resolve("TEMP_1.csv"), Arrays.asList("banana,2", "elderberry,5"));
        Files.write(stagingDir.resolve("TEMP_2.csv"), Arrays.asList("cherry,3", "fig,6"));

        MergeFiles mergeFiles = new MergeFiles(1, stagingDir.toString(), outputFile.toString(), 1);
        mergeFiles.execute();

        assertTrue(Files.exists(outputFile));
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(6, lines.size());
        // Verify sorted order
        for (int i = 1; i < lines.size(); i++) {
            String prev = lines.get(i - 1).split(",")[0];
            String curr = lines.get(i).split(",")[0];
            assertTrue(prev.compareTo(curr) <= 0,
                    "Output should be sorted but found '" + prev + "' before '" + curr + "'");
        }
    }

    @Test
    void execute_shouldMergeFourFilesWithParallelism() throws IOException, InterruptedException {
        Files.write(stagingDir.resolve("TEMP_0.csv"), Collections.singletonList("apple,1"));
        Files.write(stagingDir.resolve("TEMP_1.csv"), Collections.singletonList("cherry,3"));
        Files.write(stagingDir.resolve("TEMP_2.csv"), Collections.singletonList("banana,2"));
        Files.write(stagingDir.resolve("TEMP_3.csv"), Collections.singletonList("date,4"));

        MergeFiles mergeFiles = new MergeFiles(1, stagingDir.toString(), outputFile.toString(), 2);
        mergeFiles.execute();

        assertTrue(Files.exists(outputFile));
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(4, lines.size());
        // Verify sorted order
        for (int i = 1; i < lines.size(); i++) {
            String prev = lines.get(i - 1).split(",")[0];
            String curr = lines.get(i).split(",")[0];
            assertTrue(prev.compareTo(curr) <= 0,
                    "Output should be sorted but found '" + prev + "' before '" + curr + "'");
        }
    }

    @Test
    void execute_shouldCleanUpIntermediateFiles() throws IOException, InterruptedException {
        Files.write(stagingDir.resolve("TEMP_0.csv"), Arrays.asList("apple,1", "cherry,3"));
        Files.write(stagingDir.resolve("TEMP_1.csv"), Arrays.asList("banana,2", "date,4"));

        MergeFiles mergeFiles = new MergeFiles(1, stagingDir.toString(), outputFile.toString(), 1);
        mergeFiles.execute();

        // Staging dir should have exactly 1 file remaining (the final merged file)
        long fileCount = Files.list(stagingDir).count();
        assertEquals(1, fileCount, "Staging directory should have exactly 1 file after merge");
    }
}
