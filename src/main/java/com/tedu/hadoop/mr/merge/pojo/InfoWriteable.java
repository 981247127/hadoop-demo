package com.tedu.hadoop.mr.merge.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class InfoWriteable implements WritableComparable<InfoWriteable> {
	private Order order;
	private Product product;

	public String getKey() {
		Long oid = Optional.ofNullable(this.order).orElseGet(() -> new Order()).getId();
		String pid = Optional.ofNullable(this.product).orElseGet(() -> new Product()).getId();
		return oid + "-" + pid;
	}

	public Text getKeyText() {
		return new Text(this.getKey());
	}

	public Order getOrder() {
		return order;
	}

	public void setOrder(Order order) {
		this.order = order;
	}

	public Product getProduct() {
		return product;
	}

	public void setProduct(Product product) {
		this.product = product;
	}

	@Override
	public int compareTo(InfoWriteable infoWriteable) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(JSON.toJSONString(this.order));
		out.writeUTF(JSON.toJSONString(this.product));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.order = JSON.parseObject(in.readUTF(), Order.class);
		this.product = JSON.parseObject(in.readUTF(), Product.class);
	}

	@Override
	public String toString() {
		Order o = Optional.ofNullable(this.order).orElse(new Order());
		Product p = Optional.ofNullable(this.product).orElse(new Product());
		return String.format("%d, %s, %f, %s, %s, %d, %f",
				o.getId(), o.getDate(), o.getAmount(), o.getPid(), p.getName(), p.getCategoryId(), p.getPrice()
		);
	}
}
