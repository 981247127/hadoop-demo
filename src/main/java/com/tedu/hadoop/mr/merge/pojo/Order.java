package com.tedu.hadoop.mr.merge.pojo;

public class Order {
	// 下标位置：0，订单ID
	private Long id;
	// 下标位置：1，订单日期
	private String date;
	// 下标位置：2，商品ID
	private String pid;
	// 下标位置：3，订单价格
	private Double amount;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	/**
	 * 解析字符串为 Order 对象
	 * @param strings 字符串数组，以逗号为切割符之后得到的数组
	 */
	public static Order get(String[] strings) {
		if (strings.length > 3) {
			Order order = new Order();
			// 下标位置：0，订单ID
			try {
				order.setId(Long.valueOf(strings[0]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			// 下标位置：1，订单日期
			order.setDate(strings[1]);
			// 下标位置：2，商品ID
			order.setPid(strings[2]);
			// 下标位置：3，订单价格
			try {
				order.setAmount(Double.valueOf(strings[3]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			return order;
		}
		return null;
	}
}