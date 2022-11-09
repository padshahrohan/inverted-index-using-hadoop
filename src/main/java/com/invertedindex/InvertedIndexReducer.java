package com.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuilder answer = new StringBuilder("");

        for (Text value : values) {
            answer.append(",");
            answer.append(value.toString());
        }

        if (answer.length() >= 1) {
            answer.replace(0, 1, "");
        }
        context.write(new Text(key), new Text(answer.toString()));
    }
}
