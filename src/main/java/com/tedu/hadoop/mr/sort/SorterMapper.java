package com.tedu.hadoop.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SorterMapper extends Mapper<LongWritable, Text, SorterWritable, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strings = value.toString().split("\t");
		if (strings.length > 1) {
			SorterWritable writable = new SorterWritable();
			writable.setName(strings[0]);
			writable.setValue(Integer.valueOf(strings[1]));
			context.write(writable, NullWritable.get());
		}
	}
}
