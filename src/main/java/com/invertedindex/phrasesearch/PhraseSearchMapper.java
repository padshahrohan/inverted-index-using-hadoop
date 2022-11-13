package com.invertedindex.phrasesearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PhraseSearchMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<String> words = Arrays.stream(conf.getStrings("words"))
                .map(String::toLowerCase).collect(Collectors.toList());

        String[] split = value.toString().split("\\t");
        String word = split[0];

        if (words.contains(word)) {
            System.out.println("split " + split[1]);
            String occurrences = split[1];
            context.write(new IntWritable(1), new Text(occurrences));
        }
    }

}