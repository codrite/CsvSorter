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

            CountDownLatch countDownLatch = new CountDownLatch(files.size() / 2);
            for (int i = 0; (i + 1) < files.size(); i += 2)
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
            merge(firstFile, secondFile);
        }

        public void merge(Path first, Path second) {
            final Path fileName = Paths.get(SPLIT_INDEX_FILE_DIR + INDEX_FILE_NAME + "_" + System.currentTimeMillis());

            try {
                long firstFileRows = getFileRowCount(first);
                long secondFileRows = getFileRowCount(second);

                try (BufferedReader firstFileBufferedReader = Files.newBufferedReader(first);
                     BufferedReader secondFileBufferedReader = Files.newBufferedReader(second);
                     BufferedWriter bufferedWriter = Files.newBufferedWriter(fileName)) {

                    String fRec = firstFileBufferedReader.readLine();
                    String sRec = secondFileBufferedReader.readLine();
                    if (firstFileRows >= secondFileRows) {
                        do {
                            if (fRec.compareToIgnoreCase(sRec) >= 0) {
                                writeToFile(bufferedWriter, sRec);
                                sRec = secondFileBufferedReader.readLine();
                            } else {
                                writeToFile(bufferedWriter, fRec);
                                fRec = firstFileBufferedReader.readLine();
                            }
                        } while(fRec != null && sRec != null);
                    }

                    while(sRec != null) {
                        writeToFile(bufferedWriter, sRec);
                        sRec = secondFileBufferedReader.readLine();
                    }

                    while(fRec != null) {
                        writeToFile(bufferedWriter, fRec);
                        fRec = firstFileBufferedReader.readLine();
                    }

                } finally {
                    Files.deleteIfExists(first);
                    Files.deleteIfExists(second);
                }
            } catch (IOException ioException) {
                log.error(ioException.getMessage());
                throw new IllegalArgumentException(ioException);
            } finally {
                countDownLatch.countDown();
            }
        }

        private long getFileRowCount(Path file) throws IOException {
            try (BufferedReader bufferedReader = Files.newBufferedReader(file)) {
                return bufferedReader.lines().count();
            }
        }

        private void writeToFile(BufferedWriter fileName, String line) {
            try {
                fileName.write(line);
                fileName.newLine();
            } catch (IOException ioException) {
                log.error(ioException.getMessage());
            }
        }

    }

}
