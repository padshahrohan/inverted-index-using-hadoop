package com.invertedindex.positivityrank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\t");
        String fileName = split[0];
        String numberOfPositiveWords = split[1];

        context.write(new IntWritable(1), new Text(fileName + "_" + numberOfPositiveWords));
    }

}