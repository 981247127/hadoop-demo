package com.tedu.hadoop.mr.wc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCounterJob {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(WordCounterJob.class);

		job.setMapperClass(WordCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://h201.c8:8020/test/wc.txt"));

		job.setReducerClass(WordCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://h201.c8:8020/out/wc"));

		job.waitForCompletion(true);
	}
}