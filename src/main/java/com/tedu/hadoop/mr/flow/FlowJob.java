package com.tedu.hadoop.mr.flow;

import com.tedu.hadoop.mr.common.ConfigurationManager;
import com.tedu.hadoop.mr.common.HadoopDistributionFileSystem;
import com.tedu.hadoop.mr.flow.sorter.FlowSorterComparable;
import com.tedu.hadoop.mr.flow.sorter.FlowSorterMapper;
import com.tedu.hadoop.mr.flow.summary.FlowPhonePartitioner;
import com.tedu.hadoop.mr.flow.summary.FlowSummaryMapper;
import com.tedu.hadoop.mr.flow.summary.FlowSummaryReducer;
import com.tedu.hadoop.mr.flow.summary.FlowSummaryWritableComparable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowJob {
	public static void main(String[] args) throws Exception {
		// 案例 3
		boolean u3 = args != null && args.length > 0 && "3".equals(args[0]);
		// 案例 2
		boolean u2 = args != null && args.length > 0 && "2".equals(args[0]);
		HadoopDistributionFileSystem.rm("/out");

		// 案例 1 和 3
		boolean successed = false;
		{
			Job job = Job.getInstance(ConfigurationManager.get());
			job.setJarByClass(FlowJob.class);

			// 案例 1 的 Mapper
			job.setMapperClass(FlowSummaryMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowSummaryWritableComparable.class);

			// 案例 1 的 Reducer
			job.setReducerClass(FlowSummaryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			if (u3) {
				// 案例 3 的 Partitioner
				job.setPartitionerClass(FlowPhonePartitioner.class);
				job.setNumReduceTasks(4);
			}


			FileInputFormat.setInputPaths(job, new Path("/test/data_flow.dat"));
			FileOutputFormat.setOutputPath(job, new Path("/out/flow"));

			successed = job.waitForCompletion(true);
		}

		if (successed && u2) {
			// 案例 2 的输入是案例 1 的输出
			Job job = Job.getInstance(ConfigurationManager.get());
			job.setJarByClass(FlowJob.class);

			// 案例 2 的 Mapper
			job.setMapperClass(FlowSorterMapper.class);
			job.setOutputKeyClass(FlowSorterComparable.class);
			job.setOutputValueClass(Text.class);

			// 案例 2 的 Reducer

			FileInputFormat.setInputPaths(job, new Path("/out/flow/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path("/out/flow-sorter"));

			job.waitForCompletion(true);
		}
	}
}
