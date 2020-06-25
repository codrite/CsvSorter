package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class GenerateCSVIndexFile {

    final static String SPLIT_INDEX_FILE_DIR = "index/";
    final static String INDEX_FILE_NAME = "index/FINAL";

    ExecutorService executorService;

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

    public GenerateCSVIndexFile(int numberOfThreads) {
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
    }

    public void execute() throws IOException, InterruptedException {
        Set<Path> processed = new HashSet<>();

        List<Path> files = Files.list(Paths.get(SPLIT_INDEX_FILE_DIR)).collect(Collectors.toList());

        while (true) {
            List<Path> filesToMerge = files.stream().filter(p -> !processed.contains(p)).limit(2).collect(Collectors.toList());
            if (filesToMerge.size() == 1)
                break;

            CountDownLatch countDownLatch = new CountDownLatch(1);
            executorService.submit(new MergeFiles(filesToMerge.get(0), filesToMerge.get(1), countDownLatch));
            countDownLatch.await();

            processed.add(filesToMerge.get(0));
            processed.add(filesToMerge.get(1));

            files = Files.list(Paths.get(SPLIT_INDEX_FILE_DIR)).collect(Collectors.toList());
        }

        for (Path path : processed) {
            Files.deleteIfExists(path);
        }
    }

    public boolean isComplete() {
        return executorService.isTerminated();
    }

    public class MergeFiles implements Runnable {

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
            merge(firstFile, secondFile, INDEX_FILE_NAME);
        }

        public void merge(Path first, Path second, String destination) {
            try {
                if (Files.exists(Paths.get(destination)))
                    destination = destination + "_" + System.currentTimeMillis();

                //long firstFileRows = getFileRowCount(first);
                //long secondFileRows = getFileRowCount(second);

                try (BufferedReader firstFileBufferedReader = Files.newBufferedReader(first);
                     BufferedReader secondFileBufferedReader = Files.newBufferedReader(second);
                     BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(destination))) {

                    String fRec = firstFileBufferedReader.readLine();
                    String sRec = secondFileBufferedReader.readLine();
                    while (fRec != null && sRec != null) {
                        if (fRec.compareTo(sRec) >= 0) {
                            writeToFile(bufferedWriter, sRec);
                            sRec = secondFileBufferedReader.readLine();
                        } else {
                            writeToFile(bufferedWriter, fRec);
                            fRec = firstFileBufferedReader.readLine();
                        }
                    }

                    while (sRec != null) {
                        writeToFile(bufferedWriter, sRec);
                        sRec = secondFileBufferedReader.readLine();
                    }

                    while (fRec != null) {
                        writeToFile(bufferedWriter, fRec);
                        fRec = firstFileBufferedReader.readLine();
                    }

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
            reentrantReadWriteLock.writeLock().lock();
            try {
                fileName.write(line);
                fileName.newLine();
            } catch (IOException ioException) {
                log.error(ioException.getMessage());
            } finally {
                reentrantReadWriteLock.writeLock().unlock();
            }
        }

    }

}
