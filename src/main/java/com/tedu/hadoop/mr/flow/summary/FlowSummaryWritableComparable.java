package com.tedu.hadoop.mr.flow.summary;

import com.tedu.hadoop.mr.flow.sorter.FlowSorterComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowSummaryWritableComparable extends FlowSorterComparable {
	// 下标位置：0，时间戳
	private Long timestamp;
	// 下标位置：2，物理地址
	private String mac;
	// 下标位置：3，IP地址
	private String ipv4;
	// 下标位置：4，网站服务器名称
	private String hostname;
	// 下标位置：5，网站名称
	private String website;
	// 下标位置：10，状态码
	private Integer status;

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getIpv4() {
		return ipv4;
	}

	public void setIpv4(String ipv4) {
		this.ipv4 = ipv4;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return timestamp +
				"\t" + getPhone() +
				"\t" + mac +
				"\t" + ipv4 +
				"\t" + hostname +
				"\t" + website +
				"\t" + getUp() +
				"\t" + getDown() +
				"\t" + getUpTotal() +
				"\t" + getDownTotal() +
				"\t" + status;
	}

	public String toShortString() {
		return super.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// 下标位置：0，时间戳
		out.writeLong(this.timestamp);
		// 下标位置：1，手机号
		out.writeUTF(this.getPhone());
		// 下标位置：2，物理地址
		out.writeUTF(this.mac);
		// 下标位置：3，IP地址
		out.writeUTF(this.ipv4);
		// 下标位置：4，网站服务器名称
		out.writeUTF(this.hostname);
		// 下标位置：5，网站名称
		out.writeUTF(this.website);
		// 下标位置：6，上行数据包总和
		out.writeLong(this.getUp());
		// 下标位置：7，下行数据包总和
		out.writeLong(this.getDown());
		// 下标位置：8，上行总流量之和
		out.writeLong(this.getUpTotal());
		// 下标位置：9，下行总流量之和
		out.writeLong(this.getDownTotal());
		// 下标位置：10，状态码
		out.writeInt(this.status);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 下标位置：0，时间戳
		this.timestamp = in.readLong();
		// 下标位置：1，手机号
		setPhone(in.readUTF());
		// 下标位置：2，物理地址
		this.mac = in.readUTF();
		// 下标位置：3，IP地址
		this.ipv4 = in.readUTF();
		// 下标位置：4，网站服务器名称
		this.hostname = in.readUTF();
		// 下标位置：5，网站名称
		this.website = in.readUTF();
		// 下标位置：6，上行数据包总和
		setUp(in.readLong());
		// 下标位置：7，下行数据包总和
		setDown(in.readLong());
		// 下标位置：8，上行总流量之和
		setUpTotal(in.readLong());
		// 下标位置：9，下行总流量之和
		setDownTotal(in.readLong());
		// 下标位置：10，状态码
		this.status = in.readInt();
	}

	/**
	 * 解析字符串为 FlowWritableComparable 对象
	 * @param line 字符串，以制表位为切割符
	 */
	public static FlowSummaryWritableComparable analysis(String line) {
		if (line == null) {
			return null;
		}
		String[] strings = line.split("\t");
		if (strings.length > 10) {
			FlowSummaryWritableComparable flow = new FlowSummaryWritableComparable();
			// 下标位置：0，时间戳
			try {
				flow.setTimestamp(Long.valueOf(strings[0].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			// 下标位置：1，手机号
			flow.setPhone(strings[1]);
			// 下标位置：2，物理地址
			flow.setMac(strings[2]);
			// 下标位置：3，IP地址
			flow.setIpv4(strings[3]);
			// 下标位置：4，网站服务器名称
			flow.setHostname(strings[4]);
			// 下标位置：5，网站名称
			flow.setWebsite(strings[5]);
			try {
				// 下标位置：6，上行数据包总和
				flow.setUp(Long.valueOf(strings[6].trim()));
			} catch (NullPointerException | NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：7，下行数据包总和
				flow.setDown(Long.valueOf(strings[7].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：8，上行总流量之和
				flow.setUpTotal(Long.valueOf(strings[8].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：9，下行总流量之和
				flow.setDownTotal(Long.valueOf(strings[9].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			try {
				// 下标位置：10，状态码
				flow.setStatus(Integer.valueOf(strings[10].trim()));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			return flow;
		}
		return null;
	}
}
