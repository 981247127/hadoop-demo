package com.tedu.hadoop.mr.sort;

import com.tedu.hadoop.mr.partition.PartitionJob;
import com.tedu.hadoop.mr.partition.PartitionMapper;
import com.tedu.hadoop.mr.partition.PartitionPartitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SorterJob {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(SorterJob.class);

		job.setMapperClass(SorterMapper.class);
		job.setOutputKeyClass(SorterWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://h201.c8:8020/test/sort.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://h201.c8:8020/out/sort"));

		job.waitForCompletion(true);
	}
}