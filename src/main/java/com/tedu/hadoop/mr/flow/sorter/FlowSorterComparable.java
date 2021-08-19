package com.tedu.hadoop.mr.flow.sorter;

import com.tedu.hadoop.mr.flow.summary.FlowSummaryWritableComparable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowSorterComparable implements WritableComparable<FlowSorterComparable> {
	// 下标位置：1，手机号
	private String phone;
	// 下标位置：6，上行数据包总和
	private Long up;
	// 下标位置：7，下行数据包总和
	private Long down;
	// 下标位置：8，上行总流量之和
	private Long upTotal;
	// 下标位置：9，下行总流量之和
	private Long downTotal;

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Long getUp() {
		return up;
	}

	public void setUp(Long up) {
		this.up = up;
	}

	public Long getDown() {
		return down;
	}

	public void setDown(Long down) {
		this.down = down;
	}

	public Long getUpTotal() {
		return upTotal;
	}

	public void setUpTotal(Long upTotal) {
		this.upTotal = upTotal;
	}

	public Long getDownTotal() {
		return downTotal;
	}

	public void setDownTotal(Long downTotal) {
		this.downTotal = downTotal;
	}

	@Override
	public int compareTo(FlowSorterComparable writeable) {
		if (writeable == null || this.up == null || writeable.getUp() == null) {
			return 0;
		}
		long diff = writeable.getUp() - this.up;
		return diff > 0 ? 1 : diff == 0 ? 0 : -1;
	}

	public String toFriendlyString() {
		return String.format("手机号【%s】的上行数据包总和为：%d，下行数据包总和为：%d，上行总流量之和为：%d，下行总流量之和为：%d",
				phone, up, down, upTotal, downTotal);
	}

	@Override
	public String toString() {
		return String.format("%s\t%d\t%d\t%d\t%d", phone, up, down, upTotal, downTotal);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// 下标位置：1，手机号
		out.writeUTF(this.phone);
		// 下标位置：6，上行数据包总和
		out.writeLong(this.up);
		// 下标位置：7，下行数据包总和
		out.writeLong(this.down);
		// 下标位置：8，上行总流量之和
		out.writeLong(this.upTotal);
		// 下标位置：9，下行总流量之和
		out.writeLong(this.downTotal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 下标位置：1，手机号
		this.phone = in.readUTF();
		// 下标位置：6，上行数据包总和
		this.up = in.readLong();
		// 下标位置：7，下行数据包总和
		this.down = in.readLong();
		// 下标位置：8，上行总流量之和
		this.upTotal = in.readLong();
		// 下标位置：9，下行总流量之和
		this.downTotal = in.readLong();
	}

	/**
	 * 解析字符串为 FlowSorterComparable 对象
	 * @param line 字符串，以制表位为切割符
	 */
	public static FlowSorterComparable analysis(String line) {
		if (line == null) {
			return null;
		}
		String[] strings = line.split("\t");
		if (strings.length > 4) {
			FlowSorterComparable flow = new FlowSorterComparable();
			// 下标位置：0，手机号码
			flow.setPhone(strings[0].trim());
			try {
				// 下标位置：1，上行数据包总和
				flow.setUp(Long.valueOf(strings[1].trim()));
			} catch (NullPointerException | NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：2，下行数据包总和
				flow.setDown(Long.valueOf(strings[2].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：3，上行总流量之和
				flow.setUpTotal(Long.valueOf(strings[3].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：4，下行总流量之和
				flow.setDownTotal(Long.valueOf(strings[4].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			return flow;
		}
		return null;
	}
}