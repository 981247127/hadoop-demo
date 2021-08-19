package com.tedu.hadoop.mr.flow.summary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSummaryReducer extends Reducer<Text, FlowSummaryWritableComparable, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<FlowSummaryWritableComparable> values, Context context) throws IOException, InterruptedException {
		long up = 0L;
		long down = 0L;
		long upTotal = 0L;
		long downTotal = 0L;
		FlowSummaryWritableComparable writeable = null;
		for (FlowSummaryWritableComparable flow: values) {
			up += flow.getUp();
			down += flow.getDown();
			upTotal += flow.getUpTotal();
			downTotal += flow.getDownTotal();
			if (writeable == null) {
				writeable = flow;
			}
		}
		writeable.setUp(up);
		writeable.setUpTotal(upTotal);
		writeable.setDown(down);
		writeable.setDownTotal(downTotal);
		context.write(new Text(writeable.toShortString()), NullWritable.get());
	}
}