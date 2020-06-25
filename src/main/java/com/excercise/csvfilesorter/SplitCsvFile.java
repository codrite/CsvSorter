package com.excercise.csvfilesorter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SplitCsvFile {

    final static String TEMP_FILE = "split/TEMP";

    final String fileName;
    final Integer index;
    final Integer maxRecordsInMemory;

    public SplitCsvFile(String fileName, Integer index, Integer maxLimit) {
        this.fileName = fileName;
        this.index = index;
        this.maxRecordsInMemory = maxLimit;
    }

    public void execute() throws IOException {
        try(BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(fileName))) {
            List<Line> lines = new LinkedList<>();
            int fileCount = 0, lineCount = 1;
            while (bufferedReader.ready()) {
                if (lines.size() == maxRecordsInMemory) {
                    lines.sort(Comparator.naturalOrder());
                    writeToFile(fileCount++, lines);
                    lines.clear();
                }
                lines.add(new Line(bufferedReader.readLine().split(",")[index-1], lineCount++));
            }

            if (lines.size() > 0)
                writeToFile(fileCount, lines);
        }
    }

    void writeToFile(int fileCount, List<Line> lines) throws IOException {
        String tempFileName = TEMP_FILE + "_" + fileCount + ".csv";
        try(BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(tempFileName), StandardOpenOption.CREATE_NEW)){
             for(Line eachLine : lines) {
                 bufferedWriter.write(eachLine.toString());
                 bufferedWriter.newLine();
             }
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
