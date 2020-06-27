package com.excercise.csvfilesorter.step;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

@Slf4j
public class MergeFiles {

    private final Integer indexField;
    private final String inputDir;
    private final String outputFile;
    ForkJoinPool forkJoinPool;

    public MergeFiles(Integer indexField, String inputDir, String outputFile, int parallelism) {
        this.indexField = indexField;
        this.inputDir = inputDir;
        this.outputFile = outputFile;
        this.forkJoinPool = new ForkJoinPool(parallelism);
    }

    public void execute() throws IOException, InterruptedException {
        List<Path> allFiles = Files.list(Paths.get(inputDir)).collect(Collectors.toList());
        while (allFiles.size() > 1) {
            Iterator<Path> iterator = allFiles.iterator();
            int fileToProcess = allFiles.size();

            CountDownLatch countDownLatch = new CountDownLatch(fileToProcess / 2);
            while (fileToProcess >= 2) {
                Path firstFile = iterator.next();
                Path secondFile = iterator.next();
                forkJoinPool.submit(ForkJoinTask.adapt(new Merge2Files(indexField, firstFile.toFile().getPath(), secondFile.toFile().getPath(), "staging", countDownLatch)));
                fileToProcess -= 2;
            }
            countDownLatch.await();

            allFiles = Files.list(Paths.get(inputDir)).collect(Collectors.toList());
        }

        Files.copy(allFiles.get(0), Paths.get(outputFile), StandardCopyOption.REPLACE_EXISTING);
    }

}
