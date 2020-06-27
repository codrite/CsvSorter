package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class Merge2FilesTest {

    Merge2Files merge2Files = new Merge2Files(3, "dummy", "dummy", "dummy", new CountDownLatch(1));

    @Test
    public void shouldReturnTrue() {
        assertTrue(merge2Files.isGreater("boy", "apple"));
    }

    @Test
    public void shouldReturnFalse() {
        assertFalse(merge2Files.isGreater("apple", "boy"));
    }

    @Test
    public void shouldReturn3IndexInCsvString() {
        assertEquals("David", merge2Files.getIndexFieldValue("1,2,David"));
    }

    @Test
    public void shouldMerge2Files() throws IOException {
        Merge2Files merge2Files = new Merge2Files(9, "target/index/TEMP_29.csv", "target/index/TEMP_30.csv", "target", new CountDownLatch(1));
        merge2Files.execute();
    }

}