package com.tedu.hadoop.mr.flow.sorter;

import com.tedu.hadoop.mr.flow.summary.FlowSummaryWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSorterReducer extends Reducer<FlowSorterComparable, Text, FlowSorterComparable, Text> {

}
