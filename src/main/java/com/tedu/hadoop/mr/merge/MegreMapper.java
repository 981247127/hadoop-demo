package com.tedu.hadoop.mr.merge;

import com.tedu.hadoop.mr.merge.pojo.InfoWriteable;
import com.tedu.hadoop.mr.merge.pojo.Order;
import com.tedu.hadoop.mr.merge.pojo.Product;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MegreMapper extends Mapper<LongWritable, Text, Text, InfoWriteable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 识别输入的数据是否是来自 orders.txt 还是 product.txt, 有如下两种方法

		// 方法一：
		FileSplit split = (FileSplit) context.getInputSplit();
		if (split != null && split.getPath().getName().endsWith("orders.txt")) {
			// 订单信息
		} else {
			// 商品信息
		}

		String[] strings = value.toString().split(",");
		if (strings != null && strings.length > 3) {
			Order order = null;
			Product product = null;
			// 方法二：
			if (NumberUtils.isDigits(strings[0])) {
				// 订单信息
				order = Order.get(strings);
			} else {
				// 商品信息
				product = Product.get(strings);
			}
			InfoWriteable info = new InfoWriteable();
			info.setOrder(order);
			info.setProduct(product);
			context.write(new Text("o"), info);
		}
	}
}