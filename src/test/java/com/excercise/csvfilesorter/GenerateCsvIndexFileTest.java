package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class GenerateCsvIndexFileTest {

    @Test
    public void shouldMerge2FilesInSplitDirectory() throws IOException, InterruptedException {
        GenerateCSVIndexFile joinSortedIndexFile = new GenerateCSVIndexFile(1);
        joinSortedIndexFile.execute();
    }

}