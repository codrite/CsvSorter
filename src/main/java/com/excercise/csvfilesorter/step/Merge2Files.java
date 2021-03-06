package com.excercise.csvfilesorter.step;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Merge2Files implements Runnable {

    int indexField;
    String firstFile;
    String secondFile;
    String outputDir;
    CountDownLatch countDownLatch;

    public Merge2Files(int indexField, String firstFile, String secondFile, String outputDir, CountDownLatch countDownLatch) {
        this.indexField = indexField;
        this.firstFile = firstFile;
        this.secondFile = secondFile;
        this.outputDir = outputDir;
        this.countDownLatch = countDownLatch;
    }

    public void run() {
        try {
            execute();
        } catch (IOException ioException) {
            log.error(ioException.getMessage(), ioException);
        }
    }

    // Complexity o(nlog(n))
    void execute() throws IOException {
        Path outputFileName = Paths.get(outputDir + "/" + UUID.randomUUID().toString() + "_" + System.nanoTime() + ".csv");

        Path firstFilePath = Paths.get(firstFile);
        Path secondFilePath = Paths.get(secondFile);

        if(Files.notExists(firstFilePath) || Files.notExists(secondFilePath)) {
            countDownLatch.countDown();
            return;
        }

        try (BufferedReader fbReader = Files.newBufferedReader(firstFilePath);
             BufferedReader sbReader = Files.newBufferedReader(secondFilePath);
             BufferedWriter bufferedWriter = Files.newBufferedWriter(outputFileName)) {

            String lineFromFirstFile = fbReader.readLine();
            String lineFromSecondFile = sbReader.readLine();

            String firstFileFieldValue = getIndexFieldValue(lineFromFirstFile);
            String secondFileFieldValue = getIndexFieldValue(lineFromSecondFile);

            while (true) {
                if (isGreater(firstFileFieldValue, secondFileFieldValue)) {
                    writeToFile(bufferedWriter, lineFromSecondFile);
                    lineFromSecondFile = sbReader.readLine();
                    if (lineFromSecondFile == null)
                        break;
                    secondFileFieldValue = getIndexFieldValue(lineFromSecondFile);
                } else {
                    writeToFile(bufferedWriter, lineFromFirstFile);
                    lineFromFirstFile = fbReader.readLine();
                    if (lineFromFirstFile == null)
                        break;
                    firstFileFieldValue = getIndexFieldValue(lineFromFirstFile);
                }
            }

            while (lineFromFirstFile != null) {
                writeToFile(bufferedWriter, lineFromFirstFile);
                lineFromFirstFile = fbReader.readLine();
            }

            while (lineFromSecondFile != null) {
                writeToFile(bufferedWriter, lineFromSecondFile);
                lineFromSecondFile = sbReader.readLine();
            }
        } finally {
            countDownLatch.countDown();
        }
    }

    final String getIndexFieldValue(final String line) {
        return line.split(",")[indexField - 1];
    }

    final boolean isGreater(final String left, final String right) {
        return left.compareTo(right) >= 0;
    }

    final void writeToFile(BufferedWriter bufferedWriter, final String line) throws IOException {
        bufferedWriter.write(line);
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

}
