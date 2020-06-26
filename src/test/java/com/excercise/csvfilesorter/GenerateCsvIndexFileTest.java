package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class GenerateCsvIndexFileTest {

    @Test
    public void shouldMerge2FilesInSplitDirectory() throws IOException, InterruptedException {
        CreateCSVIndexFile joinSortedIndexFile = new CreateCSVIndexFile(1);
        joinSortedIndexFile.execute();
    }

}