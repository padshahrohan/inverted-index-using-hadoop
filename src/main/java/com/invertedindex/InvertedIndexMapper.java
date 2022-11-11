package com.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    final static Logger LOGGER = Logger.getLogger(InvertedIndexMapper.class);
    int lineNumber = 1;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String path = ((FileSplit) context.getInputSplit()).getPath().toString();
        LOGGER.info("Map called");
        System.out.println("Map called");
        LOGGER.info("File name " + path);
        System.out.println("File name " + path);
        StringTokenizer itr = new StringTokenizer(value.toString());

        HashSet<String> words = new HashSet<String>();
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            if (!words.contains(word)) {
                String record = path + "_" +  lineNumber;
                context.write(new Text(word), new Text(record));
                words.add(word);
            }
        }
        lineNumber++;
    }
}