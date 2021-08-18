package com.tedu.hadoop.mr.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionPartitioner extends Partitioner<Text, NullWritable> {

	public int getPartition(Text text, NullWritable nullWritable, int i) {
		return text.toString().length() > 10 ? 1 : 0;
	}

}
