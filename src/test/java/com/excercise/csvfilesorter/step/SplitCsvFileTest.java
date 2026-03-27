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

class SplitCsvFileTest {

    @TempDir
    Path tempDir;

    private Path stagingDir;

    @BeforeEach
    void setUp() throws IOException {
        stagingDir = tempDir.resolve("staging");
        Files.createDirectories(stagingDir);
    }

    // --- Line.compareTo tests ---

    @Test
    void compareTo_shouldReturnNegativeWhenValueIsLess() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        SplitCsvFile.Line b = splitter.new Line("banana", 2, "banana,2");
        assertTrue(a.compareTo(b) < 0);
    }

    @Test
    void compareTo_shouldReturnPositiveWhenValueIsGreater() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("banana", 1, "banana,1");
        SplitCsvFile.Line b = splitter.new Line("apple", 2, "apple,2");
        assertTrue(a.compareTo(b) > 0);
    }

    @Test
    void compareTo_shouldReturnZeroWhenValuesAreEqual() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        SplitCsvFile.Line b = splitter.new Line("apple", 2, "apple,2");
        assertEquals(0, a.compareTo(b));
    }

    @Test
    void compareTo_shouldSortCorrectlyViaCollectionsSort() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line c = splitter.new Line("cherry", 1, "cherry,1");
        SplitCsvFile.Line a = splitter.new Line("apple", 2, "apple,2");
        SplitCsvFile.Line b = splitter.new Line("banana", 3, "banana,3");
        List<SplitCsvFile.Line> lines = Arrays.asList(c, a, b);
        Collections.sort(lines);
        assertEquals("apple", lines.get(0).value);
        assertEquals("banana", lines.get(1).value);
        assertEquals("cherry", lines.get(2).value);
    }

    // --- Line.equals and hashCode tests ---

    @Test
    void equals_shouldReturnTrueForSameValueAndLineNumber() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        SplitCsvFile.Line b = splitter.new Line("apple", 1, "apple,1");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void equals_shouldReturnFalseForDifferentValue() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        SplitCsvFile.Line b = splitter.new Line("banana", 1, "banana,1");
        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldReturnFalseForDifferentLineNumber() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        SplitCsvFile.Line b = splitter.new Line("apple", 2, "apple,2");
        assertNotEquals(a, b);
    }

    @Test
    void equals_shouldReturnFalseForNull() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 1, "apple,1");
        assertNotEquals(null, a);
    }

    // --- Line.toString test ---

    @Test
    void toString_shouldReturnValueAndLineNumber() {
        SplitCsvFile splitter = new SplitCsvFile(tempDir.resolve("dummy.csv"), 1, 10);
        SplitCsvFile.Line a = splitter.new Line("apple", 3, "apple,extra");
        assertEquals("apple,3", a.toString());
    }

    // --- execute() tests ---

    @Test
    void execute_shouldSplitFileIntoChunks() throws IOException {
        Path input = tempDir.resolve("input.csv");
        Files.write(input, Arrays.asList(
                "cherry,3,x",
                "apple,1,x",
                "banana,2,x",
                "date,4,x"
        ));

        SplitCsvFile splitter = new SplitCsvFile(input, 1, 2);
        // Override STAGING_FILE to use temp dir
        setStagingPrefix(splitter);
        splitter.execute();

        Path chunk0 = tempDir.resolve("staging/TEMP_0.csv");
        Path chunk1 = tempDir.resolve("staging/TEMP_1.csv");
        assertTrue(Files.exists(chunk0), "First chunk should exist");
        assertTrue(Files.exists(chunk1), "Second chunk should exist");

        // Each chunk should be sorted
        List<String> chunk0Lines = Files.readAllLines(chunk0);
        assertEquals(2, chunk0Lines.size());
        assertTrue(chunk0Lines.get(0).startsWith("apple") || chunk0Lines.get(0).startsWith("cherry"));
        // First chunk: cherry, apple -> sorted: apple, cherry
        assertEquals("apple,1,x", chunk0Lines.get(0));
        assertEquals("cherry,3,x", chunk0Lines.get(1));

        // Second chunk: banana, date -> sorted: banana, date
        List<String> chunk1Lines = Files.readAllLines(chunk1);
        assertEquals(2, chunk1Lines.size());
        assertEquals("banana,2,x", chunk1Lines.get(0));
        assertEquals("date,4,x", chunk1Lines.get(1));
    }

    @Test
    void execute_shouldHandleSingleLineFile() throws IOException {
        Path input = tempDir.resolve("single.csv");
        Files.write(input, Collections.singletonList("only,1,line"));

        SplitCsvFile splitter = new SplitCsvFile(input, 1, 10);
        setStagingPrefix(splitter);
        splitter.execute();

        Path chunk0 = tempDir.resolve("staging/TEMP_0.csv");
        assertTrue(Files.exists(chunk0));
        List<String> lines = Files.readAllLines(chunk0);
        assertEquals(1, lines.size());
        assertEquals("only,1,line", lines.get(0));
    }

    @Test
    void execute_shouldHandleFewerRowsThanMaxRecords() throws IOException {
        Path input = tempDir.resolve("few.csv");
        Files.write(input, Arrays.asList(
                "banana,2",
                "apple,1"
        ));

        SplitCsvFile splitter = new SplitCsvFile(input, 1, 100);
        setStagingPrefix(splitter);
        splitter.execute();

        Path chunk0 = tempDir.resolve("staging/TEMP_0.csv");
        assertTrue(Files.exists(chunk0));
        List<String> lines = Files.readAllLines(chunk0);
        assertEquals(2, lines.size());
        assertEquals("apple,1", lines.get(0));
        assertEquals("banana,2", lines.get(1));

        // Should only produce one chunk
        assertFalse(Files.exists(tempDir.resolve("staging/TEMP_1.csv")));
    }

    @Test
    void execute_shouldHandleDuplicateValues() throws IOException {
        Path input = tempDir.resolve("dupes.csv");
        Files.write(input, Arrays.asList(
                "banana,1",
                "apple,2",
                "banana,3",
                "apple,4"
        ));

        SplitCsvFile splitter = new SplitCsvFile(input, 1, 10);
        setStagingPrefix(splitter);
        splitter.execute();

        Path chunk0 = tempDir.resolve("staging/TEMP_0.csv");
        List<String> lines = Files.readAllLines(chunk0);
        assertEquals(4, lines.size());
        // Duplicates should be grouped together in sorted order
        assertTrue(lines.get(0).startsWith("apple"));
        assertTrue(lines.get(1).startsWith("apple"));
        assertTrue(lines.get(2).startsWith("banana"));
        assertTrue(lines.get(3).startsWith("banana"));
    }

    @Test
    void execute_shouldSortByCorrectColumn() throws IOException {
        Path input = tempDir.resolve("cols.csv");
        Files.write(input, Arrays.asList(
                "1,cherry,x",
                "2,apple,y",
                "3,banana,z"
        ));

        // Sort by column 2
        SplitCsvFile splitter = new SplitCsvFile(input, 2, 10);
        setStagingPrefix(splitter);
        splitter.execute();

        Path chunk0 = tempDir.resolve("staging/TEMP_0.csv");
        List<String> lines = Files.readAllLines(chunk0);
        assertEquals("2,apple,y", lines.get(0));
        assertEquals("3,banana,z", lines.get(1));
        assertEquals("1,cherry,x", lines.get(2));
    }

    /**
     * Helper to redirect STAGING_FILE to the temp directory so tests don't
     * write into the project's staging folder.
     */
    private void setStagingPrefix(SplitCsvFile splitter) throws IOException {
        try {
            java.lang.reflect.Field field = SplitCsvFile.class.getDeclaredField("STAGING_FILE");
            field.setAccessible(true);

            // Remove final modifier
            java.lang.reflect.Field modifiers = java.lang.reflect.Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);

            field.set(null, stagingDir.toString() + "/TEMP");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to set staging prefix for test", e);
        }
    }
}
