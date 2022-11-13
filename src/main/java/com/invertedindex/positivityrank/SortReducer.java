package com.invertedindex.positivityrank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SortReducer extends Reducer<IntWritable, Text,  Text, LongWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        List<FileNameAndPositiveWords> fileNameAndPositiveWords = new ArrayList<>();

        for (Text value : values) {
            System.out.println("values " + value);
            String[] fileAndLineNumber = value.toString().split(":=:");
            fileNameAndPositiveWords.add(new FileNameAndPositiveWords(fileAndLineNumber[0], Long.parseLong(fileAndLineNumber[1])));
        }

        List<FileNameAndPositiveWords> rankedFiles = fileNameAndPositiveWords.stream()
                .sorted(Comparator.comparing(FileNameAndPositiveWords::getNumberOfPositiveWords).reversed())
                .collect(Collectors.toList());

        for (FileNameAndPositiveWords rankedFile : rankedFiles) {
            context.write(new Text(rankedFile.getFileName()), new LongWritable(rankedFile.getNumberOfPositiveWords()));
        }
    }

    static class FileNameAndPositiveWords {
        String fileName;
        long numberOfPositiveWords;

        public FileNameAndPositiveWords(String fileName, long numberOfPositiveWords) {
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
