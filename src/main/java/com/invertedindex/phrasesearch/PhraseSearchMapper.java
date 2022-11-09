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

public class PhraseSearchMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    final static Logger LOGGER = Logger.getLogger(PhraseSearchMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<String> words = Arrays.asList(conf.getStrings("words"));

        String[] split = value.toString().split("\\t");
        String word = split[0];
        String occurrencesInFile = split[1];

        if (words.contains(word)) {
            System.out.println("Words " + words);
            LOGGER.info("Words" + words);
            context.write(new IntWritable(1), new Text(occurrencesInFile));
        }
    }

}