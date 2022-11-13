package com.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String answer = Stream.of(values).map(Object::toString).collect(Collectors.joining(","));
        context.write(new Text(key), new Text(answer));
    }
}
