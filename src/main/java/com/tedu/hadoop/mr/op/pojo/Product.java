package com.tedu.hadoop.mr.op.pojo;

public class Product {
	// 下标位置：0，商品ID
	private String id;
	// 下标位置：1，商品名称
	private String name;
	// 下标位置：2，商品类目ID
	private Integer categoryId;
	// 下标位置：3，商品价格
	private Double price;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(Integer categoryId) {
		this.categoryId = categoryId;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	/**
	 * 解析字符串为 Product 对象
	 * @param strings 字符串数组，以逗号为切割符之后得到的数组
	 */
	public static Product get(String[] strings) {
		if (strings.length > 3) {
			Product product = new Product();
			// 下标位置：0，商品ID
			product.setId(strings[0]);
			// 下标位置：1，商品名称
			product.setName(strings[1]);
			// 下标位置：2，商品类目ID
			try {
				product.setCategoryId(Integer.valueOf(strings[2]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			// 下标位置：3，商品价格
			try {
				product.setPrice(Double.valueOf(strings[3]));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			return product;
		}
		return null;
	}
}
