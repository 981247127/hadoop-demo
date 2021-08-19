package com.tedu.hadoop.mr.op;

import com.tedu.hadoop.mr.op.pojo.InfoWriteable;
import com.tedu.hadoop.mr.op.pojo.Order;
import com.tedu.hadoop.mr.op.pojo.Product;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OPReducer extends Reducer<Text, InfoWriteable, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<InfoWriteable> values, Context context) throws IOException, InterruptedException {
		List<Order> orders = new ArrayList<>();
		List<Product> products = new ArrayList<>();
		// 查找所有的 order
		for (InfoWriteable info : values) {
			if (info != null) {
				if (info.getOrder() != null) {
					orders.add(info.getOrder());
				}
				if (info.getProduct() != null) {
					products.add(info.getProduct());
				}
			}
		}
		for (Order order : orders) {
			String pid = order.getPid();
			if (pid != null){
				for (Product product : products) {
					if (product != null && pid.equals(product.getId())) {
						// 订单关联商品
						InfoWriteable info = new InfoWriteable();
						info.setOrder(order);
						info.setProduct(product);
						context.write(new Text(info.toString()), NullWritable.get());
					}
				}
			}
		}
	}
}
