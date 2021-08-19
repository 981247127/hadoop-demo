package com.tedu.hadoop.mr.flow.sorter;

import com.tedu.hadoop.mr.flow.summary.FlowSummaryWritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSorterMapper extends Mapper<LongWritable, Text, FlowSorterComparable, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FlowSorterComparable flow = FlowSorterComparable.analysis(value.toString());
		if (flow != null) {
			context.write(flow, new Text(flow.getPhone()));
		}
	}
}
