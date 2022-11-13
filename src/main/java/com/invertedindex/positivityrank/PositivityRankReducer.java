package com.invertedindex.positivityrank;

import com.google.common.collect.Sets;
import com.invertedindex.phrasesearch.PhaseSearchReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PositivityRankReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long rank = StreamSupport.stream(values.spliterator(), false).mapToLong(LongWritable::get).sum();
        LongWritable result = new LongWritable();
        result.set(rank);
        System.out.println("rank" + rank);
        context.write(new Text(key), result);
    }
}
