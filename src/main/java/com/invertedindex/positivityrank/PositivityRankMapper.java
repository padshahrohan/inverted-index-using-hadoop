package com.invertedindex.positivityrank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PositivityRankMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<String> positiveWords = Arrays.asList(conf.getStrings("positiveWords"));

        String[] split = value.toString().split("\\t");
        String word = split[0];

        if (positiveWords.contains(word)) {
            String[] occurrences = split[1].split(",");

            for (String wordMetadata : occurrences) {
                System.out.println("Word " + word);
                System.out.println("File and line number " + wordMetadata);
                String fileName = wordMetadata.split(":=:")[0];
                context.write(new Text(fileName), new IntWritable(1));
            }
        }
    }

}