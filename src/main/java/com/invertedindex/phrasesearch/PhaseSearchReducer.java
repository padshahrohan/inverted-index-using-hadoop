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
            System.out.println("value" + value.toString());
            String[] occurrences = value.toString().split(",");
            Set<String> fileNameAndLineNumbers = Arrays.stream(occurrences)
                    .map(occurrence -> {
                        String[] wordMetadata = occurrence.split(",");
                        String fileNameAndLineNumber = wordMetadata[0] + ":=:" + wordMetadata[1];
                        Set<String> wordOffsets = fileNameAndLineNumberToWordOffset.getOrDefault(fileNameAndLineNumber, new HashSet<>());
                        wordOffsets.add(wordMetadata[2]);
                        fileNameAndLineNumberToWordOffset.put(fileNameAndLineNumber, wordOffsets);
                        return fileNameAndLineNumber;
                    }).collect(Collectors.toSet());

            if (OR.equalsIgnoreCase(operator)) {
                phraseSearchOutput = Sets.union(phraseSearchOutput, fileNameAndLineNumbers);
            } else {
                if (phraseSearchOutput.isEmpty()) {
                    phraseSearchOutput.addAll(fileNameAndLineNumbers);
                } else {
                    phraseSearchOutput = Sets.intersection(phraseSearchOutput, fileNameAndLineNumbers);
                }
            }
        }
        System.out.println("final" + phraseSearchOutput);
    }

    @Override
    protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> answer = new HashSet<>();
        for (String fileNameAndLineNumber : phraseSearchOutput) {
            Set<String> wordOffsets = fileNameAndLineNumberToWordOffset.get(fileNameAndLineNumber);
            Set<String> wordMetadata = wordOffsets.stream().map(offset -> fileNameAndLineNumber + ":=:" + offset).collect(Collectors.toSet());
            answer.addAll(wordMetadata);
        }
        JobOutputBuilder jobOutputBuilder = new JobOutputBuilder(context.getConfiguration());
        context.write(new Text("Output"), new Text(jobOutputBuilder.buildOutput(answer)));
    }
}
