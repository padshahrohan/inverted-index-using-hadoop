package com.invertedindex.phrasesearch;


import com.invertedindex.InvertedIndexMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import java.util.ArrayList;
import java.util.Arrays;

public class PhraseSearchRunner extends Configured implements Tool {

    final static Logger LOGGER = Logger.getLogger(PhraseSearchRunner.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PhraseSearchRunner(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        ArrayList<String> list = new ArrayList<>(Arrays.asList(args).subList(2, args.length - 1));
        System.out.println("List of words" + list);
        LOGGER.info("List of words " + list);
        String[] words = new String[list.size()];
        words = list.toArray(words);

        conf.setStrings("words", words);
        conf.set("operator", args[args.length-1]);

        Job job = Job.getInstance(conf, "Phrase Search Word");
        job.setJarByClass(PhraseSearchRunner.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PhraseSearchMapper.class);
        job.setReducerClass(PhaseSearchReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long finishTime = job.getStatus().getFinishTime();
        LOGGER.info("Job Finished in " + (finishTime/1000)+" seconds");
        System.out.println("Job Finished in " + finishTime + " seconds");
        return (success ? 0 : 1);
    }

}