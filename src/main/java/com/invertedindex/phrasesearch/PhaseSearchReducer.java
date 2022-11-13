package com.invertedindex.phrasesearch;

import com.google.common.collect.Sets;
import com.invertedindex.JobOutputBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PhaseSearchReducer extends Reducer<IntWritable, Text, Text, Text> {
    public static final String OR = "or";
    Set<String> phraseSearchOutput = new HashSet<>();
    HashMap<String, Set<String>> fileNameAndLineNumberToWordOffset = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String operator = conf.get("operator");
        System.out.println("Operator" + operator);

        for (Text value : values) {
            String[] occurrences = value.toString().split(",");
            Set<String> fileNameAndLineNumbers = Arrays.stream(occurrences)
                    .map(occurrence -> {
                        System.out.println("occurrence" + occurrence);
                        String[] wordMetadata = occurrence.split(":=:");
                        String fileNameAndLineNumber = wordMetadata[0] + ":=:" + wordMetadata[1];
                        Set<String> wordOffsets = fileNameAndLineNumberToWordOffset.getOrDefault(fileNameAndLineNumber, new HashSet<>());
                        wordOffsets.add(wordMetadata[2]);
                        fileNameAndLineNumberToWordOffset.put(fileNameAndLineNumber, wordOffsets);
                        return fileNameAndLineNumber;
                    }).collect(Collectors.toSet());

            if (OR.equalsIgnoreCase(operator)) {
                phraseSearchOutput.addAll(fileNameAndLineNumbers);
            } else {
                if (phraseSearchOutput.isEmpty()) {
                    phraseSearchOutput.addAll(fileNameAndLineNumbers);
                } else {
                    phraseSearchOutput = phraseSearchOutput.stream().filter(fileNameAndLineNumbers::contains).collect(Collectors.toSet());
                }
            }
        }
        System.out.println("final" + phraseSearchOutput);
    }

    @Override
    protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> answer = new HashSet<>();
        for (String fileNameAndLineNumber : phraseSearchOutput) {
            String offsets = String.join(", " , fileNameAndLineNumberToWordOffset.get(fileNameAndLineNumber));
            answer.add(fileNameAndLineNumber + ":=:" + offsets);
        }
        JobOutputBuilder jobOutputBuilder = new JobOutputBuilder(context.getConfiguration());
        context.write(new Text(), new Text(jobOutputBuilder.buildOutput(answer)));
    }
}
