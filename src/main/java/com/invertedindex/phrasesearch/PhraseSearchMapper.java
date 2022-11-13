package com.invertedindex.phrasesearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PhraseSearchMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<String> words = Arrays.asList(conf.getStrings("words"));

        String[] split = value.toString().split("\\t");
        String word = split[0];

        if (words.contains(word)) {
            System.out.println("Words " + words);
//            String[] occurrences = split[1].split(",");
//            String answer = Stream.of(occurrences).map(occurrence -> {
//                String[] wordMetadata = occurrence.split(":=:");
//                return wordMetadata[0] + ":=:" + wordMetadata[1];
//            }).collect(Collectors.joining(","));
            String occurrences = split[1];
            context.write(new IntWritable(1), new Text(occurrences));
        }
    }

}