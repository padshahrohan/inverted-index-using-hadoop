package com.invertedindex.positivityrank;

import com.invertedindex.phrasesearch.PhaseSearchReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SortReducer extends Reducer<IntWritable, Text,  Text, LongWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        List<FileNameAndLineNumber> fileNameAndLineNumbers = new ArrayList<>();

        for (Text value : values) {
            System.out.println("values " + value);
            String[] fileAndLineNumber = value.toString().split("_");
            fileNameAndLineNumbers.add(new FileNameAndLineNumber(fileAndLineNumber[0], Long.parseLong(fileAndLineNumber[1])));
        }

        List<FileNameAndLineNumber> rankedFiles = fileNameAndLineNumbers.stream()
                .sorted(Comparator.comparing(FileNameAndLineNumber::getNumberOfPositiveWords).reversed())
                .collect(Collectors.toList());

        for (FileNameAndLineNumber rankedFile : rankedFiles) {
            context.write(new Text(rankedFile.getFileName()), new LongWritable(rankedFile.getNumberOfPositiveWords()));
        }
    }

    static class FileNameAndLineNumber {
        String fileName;
        long numberOfPositiveWords;

        public FileNameAndLineNumber(String fileName, long numberOfPositiveWords) {
            this.fileName = fileName;
            this.numberOfPositiveWords = numberOfPositiveWords;
        }

        public String getFileName() {
            return fileName;
        }

        public long getNumberOfPositiveWords() {
            return numberOfPositiveWords;
        }
    }
}
