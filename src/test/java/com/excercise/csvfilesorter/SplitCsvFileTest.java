package com.excercise.csvfilesorter;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class SplitCsvFileTest {

    @Test
    public void shouldSplitCsvFileIntoSmallFilesOf10RecordsEach() throws IOException {
        SplitCsvFile splitCsvFile = new SplitCsvFile("input/smallInput.csv", 9, 2);
        splitCsvFile.execute();
    }

}