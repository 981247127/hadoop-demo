package com.tedu.hadoop.mr.sort;

import com.tedu.hadoop.mr.common.ConfigurationManager;
import com.tedu.hadoop.mr.common.HadoopDistributionFileSystem;
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
		HadoopDistributionFileSystem.rm("/out");

		Job job = Job.getInstance(ConfigurationManager.get());
		job.setJarByClass(SorterJob.class);

		job.setMapperClass(SorterMapper.class);
		job.setOutputKeyClass(SorterWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/test/sort.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/out/sort"));

		job.waitForCompletion(true);
	}
}