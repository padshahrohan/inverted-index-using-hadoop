package com.invertedindex.wordsearch;


import com.invertedindex.InvertedIndexMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class WordSearchRunner extends Configured implements Tool {

    final static Logger LOGGER = Logger.getLogger(WordSearchRunner.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new WordSearchRunner(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        String query = args[2];
        Configuration conf = new Configuration();
        conf.set("word", query);
        Job job = Job.getInstance(conf, "Search Word");
        job.setJarByClass(WordSearchRunner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(WordSearchMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long finishTime = job.getStatus().getFinishTime();
        LOGGER.info("Job Finished in "+ (finishTime/1000) + "  seconds");
        System.out.println("Job Finished in " + (finishTime/1000) + " seconds");
        return (success ? 0 : 1);
    }
}