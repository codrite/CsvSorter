package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class Merge2FilesTest {

    Merge2Files merge2Files = new Merge2Files(3, "dummy", "dummy", "dummy", new CountDownLatch(1));

    // --- isGreater tests ---

    @Test
    void shouldReturnTrue() {
        assertTrue(merge2Files.isGreater("boy", "apple"));
    }

    @Test
    void shouldReturnFalse() {
        assertFalse(merge2Files.isGreater("apple", "boy"));
    }

    @Test
    void isGreater_shouldReturnTrueForEqualValues() {
        assertTrue(merge2Files.isGreater("apple", "apple"));
    }

    // --- getIndexFieldValue tests ---

    @Test
    void shouldReturn3IndexInCsvString() {
        assertEquals("David", merge2Files.getIndexFieldValue("1,2,David"));
    }

    @Test
    void getIndexFieldValue_shouldReturnFirstField() {
        Merge2Files m = new Merge2Files(1, "dummy", "dummy", "dummy", new CountDownLatch(1));
        assertEquals("hello", m.getIndexFieldValue("hello,world,test"));
    }

    @Test
    void getIndexFieldValue_shouldReturnMiddleField() {
        Merge2Files m = new Merge2Files(2, "dummy", "dummy", "dummy", new CountDownLatch(1));
        assertEquals("world", m.getIndexFieldValue("hello,world,test"));
    }

    // --- execute / merge tests ---

    @TempDir
    Path tempDir;

    @Test
    void execute_shouldMergeTwoSortedFiles() throws IOException {
        Path file1 = tempDir.resolve("sorted1.csv");
        Path file2 = tempDir.resolve("sorted2.csv");
        Files.write(file1, Arrays.asList("apple,1", "cherry,3"));
        Files.write(file2, Arrays.asList("banana,2", "date,4"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                file1.toString(), file2.toString(),
                tempDir.toString(), latch);
        merger.execute();

        assertEquals(0, latch.getCount(), "Latch should be counted down");

        // Find the output file (UUID-named)
        Path outputFile = findMergedOutput(file1, file2);
        assertNotNull(outputFile, "Merged output file should exist");

        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(4, lines.size());
        assertEquals("apple,1", lines.get(0));
        assertEquals("banana,2", lines.get(1));
        assertEquals("cherry,3", lines.get(2));
        assertEquals("date,4", lines.get(3));
    }

    @Test
    void execute_shouldHandleUnequalFileLengths() throws IOException {
        Path file1 = tempDir.resolve("short.csv");
        Path file2 = tempDir.resolve("long.csv");
        Files.write(file1, Collections.singletonList("banana,1"));
        Files.write(file2, Arrays.asList("apple,2", "cherry,3", "date,4"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                file1.toString(), file2.toString(),
                tempDir.toString(), latch);
        merger.execute();

        Path outputFile = findMergedOutput(file1, file2);
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(4, lines.size());
        assertEquals("apple,2", lines.get(0));
        assertEquals("banana,1", lines.get(1));
        assertEquals("cherry,3", lines.get(2));
        assertEquals("date,4", lines.get(3));
    }

    @Test
    void execute_shouldMergeFilesWithDuplicateKeys() throws IOException {
        Path file1 = tempDir.resolve("dupes1.csv");
        Path file2 = tempDir.resolve("dupes2.csv");
        Files.write(file1, Arrays.asList("apple,1", "apple,2"));
        Files.write(file2, Arrays.asList("apple,3", "banana,4"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                file1.toString(), file2.toString(),
                tempDir.toString(), latch);
        merger.execute();

        Path outputFile = findMergedOutput(file1, file2);
        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(4, lines.size());
        // All apples should come before banana
        assertTrue(lines.get(0).startsWith("apple"));
        assertTrue(lines.get(1).startsWith("apple"));
        assertTrue(lines.get(2).startsWith("apple"));
        assertEquals("banana,4", lines.get(3));
    }

    @Test
    void execute_shouldCountDownLatchWhenFirstFileDoesNotExist() throws IOException {
        Path file2 = tempDir.resolve("exists.csv");
        Files.write(file2, Collections.singletonList("data,1"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                tempDir.resolve("nonexistent.csv").toString(),
                file2.toString(),
                tempDir.toString(), latch);
        merger.execute();

        assertEquals(0, latch.getCount(), "Latch should still be counted down on missing file");
    }

    @Test
    void execute_shouldCountDownLatchWhenSecondFileDoesNotExist() throws IOException {
        Path file1 = tempDir.resolve("exists.csv");
        Files.write(file1, Collections.singletonList("data,1"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                file1.toString(),
                tempDir.resolve("nonexistent.csv").toString(),
                tempDir.toString(), latch);
        merger.execute();

        assertEquals(0, latch.getCount(), "Latch should still be counted down on missing file");
    }

    @Test
    void run_shouldDelegateToExecuteAndCountDownLatch() throws IOException {
        Path file1 = tempDir.resolve("run1.csv");
        Path file2 = tempDir.resolve("run2.csv");
        Files.write(file1, Collections.singletonList("banana,1"));
        Files.write(file2, Collections.singletonList("apple,2"));

        CountDownLatch latch = new CountDownLatch(1);
        Merge2Files merger = new Merge2Files(1,
                file1.toString(), file2.toString(),
                tempDir.toString(), latch);
        merger.run();

        assertEquals(0, latch.getCount());
        Path outputFile = findMergedOutput(file1, file2);
        assertNotNull(outputFile);
    }

    /**
     * Finds the merged output file in tempDir (excluding the two input files).
     */
    private Path findMergedOutput(Path... excludes) throws IOException {
        List<Path> excluded = Arrays.asList(excludes);
        return Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".csv"))
                .filter(p -> !excluded.contains(p))
                .findFirst()
                .orElse(null);
    }
}
