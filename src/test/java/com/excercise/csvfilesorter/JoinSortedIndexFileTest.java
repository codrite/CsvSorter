package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class JoinSortedIndexFileTest {

    @Test
    public void shouldMerge2FilesInSplitDirectory() throws IOException, InterruptedException {
        GenerateIndexFile joinSortedIndexFile = new GenerateIndexFile(1);
        joinSortedIndexFile.execute();
    }

}