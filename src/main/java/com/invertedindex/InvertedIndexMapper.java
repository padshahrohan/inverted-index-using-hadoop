package com.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    long lineNumber = 1;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        StringTokenizer itr = new StringTokenizer(value.toString());

        long wordOffset = 1;
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken().trim();
            if (word.length() > 1 && word.matches("^[A-Za-z]+$")) {
                String occurrence = fileName + ":=:" +  lineNumber + ":=:" + wordOffset;
                context.write(new Text(word.toLowerCase()), new Text(occurrence));
            }
            wordOffset++;
        }
        lineNumber++;
    }
}