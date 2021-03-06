package com.excercise.csvfilesorter.step;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;

class SplitCsvFileTest {

    @Test
    public void shouldSplitCsvFileIntoSmallFilesOf10RecordsEach() throws IOException {
        SplitCsvFile splitCsvFile = new SplitCsvFile(Paths.get("input/smallInput.csv"), 9, 2);
        splitCsvFile.execute();
    }

}