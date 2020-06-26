package com.excercise.csvfilesorter.step;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class FirstStepIsToSplitTheCsvFile {

    final static String IDX_FILE = "target/index/TEMP";

    final Path fileName;
    final Integer index;
    final Integer maxRecordsInMemory;

    public FirstStepIsToSplitTheCsvFile(Path fileName, Integer index, Integer maxLimit) {
        this.fileName = fileName;
        this.index = index;
        this.maxRecordsInMemory = maxLimit;
    }

    // Complexity - o(n) * o(nlog(n))
    public void execute() throws IOException {
        try(BufferedReader bufferedReader = Files.newBufferedReader(fileName)) {
            List<Line> lines = new LinkedList<>();
            int fileCount = 0, lineCount = 1;
            while (bufferedReader.ready()) {
                if (lines.size() == maxRecordsInMemory) {
                    Collections.sort(lines);
                    writeToFile(fileCount++, lines);
                    lines.clear();
                }
                lines.add(new Line(bufferedReader.readLine().split(",")[index-1], lineCount++));
            }

            if (lines.size() > 0) {
                Collections.sort(lines);
                writeToFile(fileCount, lines);
            }
        }
    }

    // Complexity - o(n)
    void writeToFile(int fileCount, List<Line> lines) throws IOException {
        String tempFileName = IDX_FILE + "_" + fileCount + ".csv";
        try(BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(tempFileName), StandardOpenOption.CREATE_NEW)){
             for(Line eachLine : lines) {
                 bufferedWriter.write(eachLine.toString());
                 bufferedWriter.newLine();
             }
             bufferedWriter.flush();
         }
    }

    class Line implements Comparable<Line> {

        String value;
        Integer lineNumber;

        public Line(String value, Integer lineNumber) {
            this.value = value;
            this.lineNumber = lineNumber;
        }

        @Override
        public int compareTo(Line line) {
            return this.value.compareTo(line.value);
        }

        @Override
        public String toString() {
            return value + "," + lineNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Line line = (Line) o;
            return Objects.equals(value, line.value) &&
                    Objects.equals(lineNumber, line.lineNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, lineNumber);
        }
    }

}
