package com.invertedindex.phrasesearch;

import com.google.common.collect.Sets;
import com.invertedindex.InvertedIndexRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class PhaseSearchReducer extends Reducer<IntWritable, Text, Text, Text> {
    final static Logger LOGGER = Logger.getLogger(PhaseSearchReducer.class);
    public static final String OR = "or";


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String operator = conf.get("operator");
        System.out.println("Operator" + operator);
        LOGGER.info("Operator" + operator);
        Set<String> answer = new HashSet<String>();

        for (Text value : values) {
            System.out.println("value" + value.toString());
            LOGGER.info("value" + value);
            String[] split = value.toString().split(",");
            List<String> occurrences = Arrays.asList(split);
            if (answer.isEmpty()) {
                answer.addAll(occurrences);
                continue;
            }

            if (OR.equalsIgnoreCase(operator)) {
                answer = Sets.union(answer, Sets.newHashSet(occurrences));
            } else {
                answer = Sets.intersection(answer, Sets.newHashSet(occurrences));
            }
            System.out.println("answer at end of loop" + answer);
        }
        LOGGER.info("answer" + answer);
        System.out.println("final" + answer);
        context.write(new Text("Output of " + operator), new Text(answer.toString()));
    }
}
