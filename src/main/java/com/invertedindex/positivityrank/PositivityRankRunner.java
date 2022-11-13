package com.invertedindex.positivityrank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class PositivityRankRunner extends Configured implements Tool {

    final static Logger LOGGER = Logger.getLogger(PositivityRankRunner.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PositivityRankRunner(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Path positiveWordsFilePath = new Path("/positive-words/positive-words.csv");
        FileSystem fileSystem = positiveWordsFilePath.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(positiveWordsFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        ArrayList<String> positiveWords = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            positiveWords.add(line.split(",")[1]);
        }

        System.out.println("List of words" + positiveWords);
        System.out.println("Arg" + args[0]);
        System.out.println("Arg" + args[1]);
        LOGGER.info("List of words " + positiveWords);
        String[] words = new String[positiveWords.size()];
        words = positiveWords.toArray(words);
        conf.setStrings("positiveWords", words);

        Job job = Job.getInstance(conf, "Positivity Rank");
        job.setJarByClass(PositivityRankRunner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(PositivityRankMapper.class);
        job.setReducerClass(PositivityRankReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path intermediateOutput = new Path("/intermediate-output");
        FileOutputFormat.setOutputPath(job, intermediateOutput);

        job.waitForCompletion(true);

        long finishTime = job.getStatus().getFinishTime();
        LOGGER.info("Job 1 Finished in " + (finishTime/1000)+" seconds");
        System.out.println("Job 1 Finished in " + finishTime + " seconds");

        Job job2 = Job.getInstance(conf, "Sort Ranks");
        job2.setJarByClass(PositivityRankRunner.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPaths(job2, intermediateOutput.toString());
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        boolean success = job2.waitForCompletion(true);

        finishTime = job2.getStatus().getFinishTime();
        LOGGER.info("Job 2 Finished in " + (finishTime/1000)+" seconds");
        System.out.println("Job 2 Finished in " + finishTime + " seconds");

        return (success ? 0 : 1);
    }

}