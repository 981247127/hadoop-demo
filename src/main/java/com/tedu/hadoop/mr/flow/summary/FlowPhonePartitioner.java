package com.tedu.hadoop.mr.flow.summary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPhonePartitioner extends Partitioner<Text, FlowSummaryWritableComparable> {
	@Override
	public int getPartition(Text text, FlowSummaryWritableComparable flowWriteable, int i) {
		String phone = text.toString();
		if (phone.startsWith("135")) {
			return 1;
		} else if (phone.startsWith("136")) {
			return 2;
		} else if (phone.startsWith("137")) {
			return 3;
		}
		return 0;
	}
}
