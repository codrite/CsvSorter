package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class MergeFilesTest {

    @Test
    public void shouldMergeFilesInDirectory() throws IOException, InterruptedException {
        new MergeFiles(9,"staging", "target/output.csv", 1).execute();
    }

}