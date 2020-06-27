package com.excercise.csvfilesorter;

import com.excercise.csvfilesorter.step.MergeFiles;
import com.excercise.csvfilesorter.step.SplitCsvFile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootApplication
@Slf4j
public class CsvFileSorterApplication {

    public static void main(String[] args) throws IOException, InterruptedException {
        Path inputPath = Paths.get(args[0]);
        Path outputFile = Paths.get(args[1]);
        Integer indexField = Integer.parseInt(args[2]);
        Integer maxRow = Integer.parseInt(args[3]);
        int concurrency = ((args[4] == null) ? 1 : Integer.parseInt(args[4]));
        Path indexPath = Paths.get("staging");

        prepareFolders(indexPath);

        long startTime = System.currentTimeMillis();

        new SplitCsvFile(inputPath, indexField, maxRow).execute();
        log.info("Completed split step in {}", (System.currentTimeMillis() - startTime) + " milliseconds ");

        startTime = System.currentTimeMillis();
        new MergeFiles(indexField, indexPath.toFile().getPath(), outputFile.toFile().getPath(), concurrency).execute();
        log.info("Completed building sorted CSV step in {}", (System.currentTimeMillis() - startTime) + " milliseconds ");
    }

    public static void prepareFolders(Path indexPath) throws IOException {
        if (Files.notExists(indexPath))
            Files.createDirectory(indexPath);
        else {
            Set<Path> files = Files.list(indexPath).collect(Collectors.toSet());
            for (Path eachFile : files) {
                Files.delete(eachFile);
            }
        }
    }

}
