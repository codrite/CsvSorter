package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Disabled;

import java.io.IOException;

class GenerateCsvIndexFileTest {

    @Disabled
    public void shouldMerge2FilesInSplitDirectory() throws IOException, InterruptedException {
        SecondStepIsToCreateIndex joinSortedIndexFile = new SecondStepIsToCreateIndex(1);
        joinSortedIndexFile.execute();
    }

}