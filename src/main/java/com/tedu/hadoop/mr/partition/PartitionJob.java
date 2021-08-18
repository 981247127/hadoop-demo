package com.tedu.hadoop.mr.partition;

import com.tedu.hadoop.mr.wc.WordCounterJob;
import com.tedu.hadoop.mr.wc.WordCounterMapper;
import com.tedu.hadoop.mr.wc.WordCounterReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionJob {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(PartitionJob.class);

		job.setMapperClass(PartitionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://h201.c8:8020/test/partition.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://h201.c8:8020/out/partitioner"));

		job.setPartitionerClass(PartitionPartitioner.class);
		job.setNumReduceTasks(2);

		job.waitForCompletion(true);
	}
}