package com.tedu.hadoop.mr.merge.m;

import com.tedu.hadoop.mr.common.ConfigurationManager;
import com.tedu.hadoop.mr.common.HadoopDistributionFileSystem;
import com.tedu.hadoop.mr.merge.pojo.InfoWriteable;
import com.tedu.hadoop.mr.merge.m.MegreMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class MegreJob {
	public static void main(String[] args) throws Exception {
		HadoopDistributionFileSystem.rm("/out");

		Job job = Job.getInstance(ConfigurationManager.get());
		job.setJarByClass(MegreJob.class);
		job.addCacheFile(new URI("/caches/product.txt"));

		job.setMapperClass(MegreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("/test/op/orders.txt"));

		// job.setReducerClass(MegreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/out/op"));

		job.waitForCompletion(true);
	}
}