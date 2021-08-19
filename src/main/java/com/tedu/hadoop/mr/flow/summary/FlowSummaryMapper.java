package com.tedu.hadoop.mr.flow.summary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSummaryMapper extends Mapper<LongWritable, Text, Text, FlowSummaryWritableComparable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FlowSummaryWritableComparable flow = FlowSummaryWritableComparable.analysis(value.toString());
		if (flow != null) {
			context.write(new Text(flow.getPhone()), flow);
		}
	}
}