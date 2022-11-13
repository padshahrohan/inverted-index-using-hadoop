package com.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class JobOutputBuilder {

    private final Configuration configuration;
    private static final String HDFS_INPUT_DIR_PREFIX = "/invertedindex/input/";

    public JobOutputBuilder(Configuration configuration) {
        this.configuration = configuration;
    }

    public String buildOutput(String[] occurrences) throws IOException {
        StringJoiner joiner = new StringJoiner(",");
        for (String occurrence : occurrences) {
            String finalOutput = findLine(occurrence, configuration);
            joiner.add(finalOutput);
        }
        return joiner.toString();
    }

    public String buildOutput(Set<String> fileNameAndLineNumbers) throws IOException {
        StringJoiner joiner = new StringJoiner(",");
        for (String fileNameAndLineNumber : fileNameAndLineNumbers) {
            String finalOutput = findLine(fileNameAndLineNumber, configuration);
            joiner.add(finalOutput);
        }
        return joiner.toString();
    }

    private String findLine(String occurrence, Configuration configuration) throws IOException {
        String[] occurrenceMetadata = occurrence.split(":=:");
        Path path = new Path(HDFS_INPUT_DIR_PREFIX + occurrenceMetadata[0]);
        FileSystem fileSystem = FileSystem.get(configuration);

        try (Stream<String> lines = new BufferedReader(new InputStreamReader(fileSystem.open(path))).lines()) {
            Optional<String> optionalLine = lines.skip(Integer.parseInt(occurrenceMetadata[1]) - 1).findFirst();
            if (optionalLine.isPresent()) {
                return occurrenceMetadata[0] + ":=:" + occurrenceMetadata[1] + ":=:" + occurrenceMetadata[2] + ":=:" + optionalLine.get();
            }
        }
        return  occurrenceMetadata[0] + ":=:" + occurrenceMetadata[1] + ":=:" + occurrenceMetadata[2] + ":=: Line not found";
    }
}
