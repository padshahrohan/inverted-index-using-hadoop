package com.invertedindex.positivityrank;

import com.google.common.collect.Sets;
import com.invertedindex.phrasesearch.PhaseSearchReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PositivityRankReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    final static Logger LOGGER = Logger.getLogger(PhaseSearchReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        IntWritable result = new IntWritable();
        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
        result.set(sum);
        LOGGER.info("sum" + sum);
        System.out.println("sum" + sum);
        context.write(new Text(key), result);
    }
}
