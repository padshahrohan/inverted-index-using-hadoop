package com.invertedindex.wordsearch;

import com.invertedindex.JobOutputBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordSearchMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String query = conf.get("word");
        String[] split = value.toString().split("\\t");

        String word = split[0];

        if (word.equalsIgnoreCase(query)) {
            String occurrence = split[1];
            JobOutputBuilder jobOutputBuilder = new JobOutputBuilder(context.getConfiguration());
            context.write(new Text(), new Text(jobOutputBuilder.buildOutput(occurrence.split(","))));
        }
    }


}