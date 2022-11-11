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

    public JobOutputBuilder(Configuration configuration) {
        this.configuration = configuration;
    }

    public String buildOutput(String[] fileNameAndLineNumbers) throws IOException {
        StringJoiner joiner = new StringJoiner(",");
        for (String fileNameAndLineNumber : fileNameAndLineNumbers) {
            String finalOutput = appendLineToFileNameAndLineNumber(fileNameAndLineNumber, configuration);
            joiner.add(finalOutput);
        }
        return joiner.toString();
    }

    public String buildOutput(Set<String> fileNameAndLineNumbers) throws IOException {
        StringJoiner joiner = new StringJoiner(",");
        for (String fileNameAndLineNumber : fileNameAndLineNumbers) {
            String finalOutput = appendLineToFileNameAndLineNumber(fileNameAndLineNumber, configuration);
            joiner.add(finalOutput);
        }
        return joiner.toString();
    }

    private String appendLineToFileNameAndLineNumber(String fileNameAndLineNumber, Configuration configuration) throws IOException {
        String[] metadata = fileNameAndLineNumber.split("_");
        Path path = new Path(metadata[0]);
        FileSystem fileSystem = FileSystem.get(configuration);

        try (Stream<String> lines = new BufferedReader(new InputStreamReader(fileSystem.open(path))).lines()) {
            Optional<String> optionalLine = lines.skip(Integer.parseInt(metadata[1]) - 1).findFirst();
            if (optionalLine.isPresent()) {
                return metadata[0] + "_" + metadata[1] + "_" + optionalLine.get();
            }
        }
        return metadata[0] + "_" + metadata[1];
    }
}
