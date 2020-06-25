package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class GenerateIndexFile {

    final static String SPLIT_INDEX_FILE_DIR = "split/";
    final static String INDEX_FILE_NAME = "INDEX";

    ExecutorService executorService;

    public GenerateIndexFile(int numberOfThreads) {
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
    }

    public void execute() throws IOException, InterruptedException {
        while (true) {
            List<Path> files = Files.list(Paths.get(SPLIT_INDEX_FILE_DIR)).collect(Collectors.toList());
            if (files.size() <= 1)
                break;

            CountDownLatch countDownLatch = new CountDownLatch(files.size()/2);
            for (int i = 0; (i+1) < files.size(); i += 2)
                executorService.submit(new MergeFiles(files.get(i), files.get(i + 1), countDownLatch));

            countDownLatch.await();
        }
    }

    private class MergeFiles implements Runnable {

        Path firstFile;
        Path secondFile;
        CountDownLatch countDownLatch;

        public MergeFiles(Path firstFile, Path secondFile, CountDownLatch countDownLatch) {
            this.firstFile = firstFile;
            this.secondFile = secondFile;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(SPLIT_INDEX_FILE_DIR + INDEX_FILE_NAME + "_" + System.currentTimeMillis()))) {
                try (BufferedReader firstBufferedReader = Files.newBufferedReader(firstFile)) {

                    boolean secondFileProcessComplete = false;
                    boolean firstFileProcessComplete = false;
                    while (firstBufferedReader.ready()) {

                        String firstLine = firstBufferedReader.readLine();
                        String firstString = firstLine.split(",")[0];

                        if (!secondFileProcessComplete) {

                            try (BufferedReader secondBufferedReader = Files.newBufferedReader(secondFile)) {

                                while (secondBufferedReader.ready()) {
                                    String secondLine = secondBufferedReader.readLine();
                                    String secondString = secondLine.split(",")[0];

                                    while (!firstFileProcessComplete) {
                                        if (secondString.compareTo(firstString) > 0) {
                                            bufferedWriter.write(firstLine);
                                            bufferedWriter.newLine();
                                            if (!firstBufferedReader.ready()) {
                                                firstFileProcessComplete = true;
                                                break;
                                            }

                                            firstLine = firstBufferedReader.readLine();
                                            firstString = firstLine.split(",")[0];
                                        } else {
                                            bufferedWriter.write(secondLine);
                                            bufferedWriter.newLine();
                                            if (!secondBufferedReader.ready())
                                                break;

                                            secondLine = secondBufferedReader.readLine();
                                            secondString = secondLine.split(",")[0];
                                        }
                                    }

                                    if(firstFileProcessComplete) {
                                        bufferedWriter.write(secondLine);
                                        bufferedWriter.newLine();
                                    }

                                }
                                secondFileProcessComplete = true;
                            }

                        }

                        if(!firstFileProcessComplete) {
                            bufferedWriter.write(firstLine);
                            bufferedWriter.newLine();
                        }

                    }
                }
            } catch (IOException ioException) {
                log.error("There was an error during reading/writting of intermediate outputs - {}", ioException.getMessage());
            }
            try {
                Files.delete(firstFile);
                Files.delete(secondFile);
            } catch (IOException ioException) {
                log.error("There was an error during deletion of intermediate outputs - {}", ioException.getMessage());
            } finally {
                countDownLatch.countDown();
            }
        }
    }

}
