package com.excercise.csvfilesorter.step;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class MergeFiles {

    private final Integer indexField;
    private final String inputDir;
    private final String outputFile;
    ExecutorService executorService;

    public MergeFiles(Integer indexField, String inputDir, String outputFile, int parallelism) {
        this.indexField = indexField;
        this.inputDir = inputDir;
        this.outputFile = outputFile;
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }

    // Complexity o(n^2)
    public void execute() throws IOException, InterruptedException {
        Set<Path> allFiles = Files.list(Paths.get(inputDir)).collect(Collectors.toSet());
        Set<Path> removeFiles = new HashSet<>();
        while (allFiles.size() > 1) {
            Iterator<Path> iterator = allFiles.iterator();
            int fileToProcess = allFiles.size();

            CountDownLatch countDownLatch = new CountDownLatch(fileToProcess / 2);
            while (fileToProcess >= 2) {
                Path firstFile = iterator.next();
                Path secondFile = iterator.next();
                executorService.submit(new Merge2Files(indexField, firstFile.toFile().getPath(), secondFile.toFile().getPath(), "staging", countDownLatch));
                fileToProcess -= 2;

                removeFiles.add(firstFile);
                removeFiles.add(secondFile);
            }
            countDownLatch.await();

            for (Path eachFile : removeFiles)
                Files.delete(eachFile);

            removeFiles.clear();
            allFiles = Files.list(Paths.get(inputDir)).collect(Collectors.toSet());
        }
        executorService.shutdown();

        Files.copy(allFiles.iterator().next(), Paths.get(outputFile), StandardCopyOption.REPLACE_EXISTING);

    }

}
