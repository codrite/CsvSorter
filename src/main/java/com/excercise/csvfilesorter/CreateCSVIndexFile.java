package com.excercise.csvfilesorter;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*
 * Create an index file for sorting the (input) CSV source.
 *
 * Each row of the index file (also csv file) has a key (index element)
 * and line number of the index element in the input CSV.
 *
 * The key is sorted (natural/ascending order) in the Index file.
 */

@Slf4j
public class CreateCSVIndexFile {

    final static String SPLIT_INDEX_FILE_DIR = "index/";
    final static String INDEX_FILE_NAME = "index/FINAL";

    ExecutorService executorService;

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    Set<Path> processed;

    public CreateCSVIndexFile(int numberOfThreads) {
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
        this.processed = new ConcurrentSkipListSet<>();
    }

    public void execute() throws IOException, InterruptedException {
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

        executorService.shutdown();
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
