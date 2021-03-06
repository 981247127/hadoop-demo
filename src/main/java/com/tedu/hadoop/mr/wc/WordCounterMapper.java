package com.tedu.hadoop.mr.wc;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words =  value.toString().split(",");
		for(String word : words) {
			context.write(new Text(word), new LongWritable(1L));
		}
	}
}
