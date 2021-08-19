package com.tedu.hadoop.mr.op;

import com.tedu.hadoop.mr.op.pojo.InfoWriteable;
import com.tedu.hadoop.mr.op.pojo.Order;
import com.tedu.hadoop.mr.op.pojo.Product;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OPMapper extends Mapper<LongWritable, Text, Text, InfoWriteable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strings = value.toString().split(",");
		if (strings != null && strings.length > 3) {
			Order order = null;
			Product product = null;
			if (NumberUtils.isDigits(strings[0])) {
				order = Order.get(strings);
			} else {
				product = Product.get(strings);
			}
			InfoWriteable info = new InfoWriteable();
			info.setOrder(order);
			info.setProduct(product);
			context.write(new Text("o"), info);
		}
	}
}