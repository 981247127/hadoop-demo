package com.tedu.hadoop.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SorterWritable implements WritableComparable<SorterWritable> {
	private String name;
	private Integer value;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public int compareTo(@Nonnull SorterWritable sorter) {
		if (sorter.getValue() == null)
			return 0;
		if (this.value == null)
			return -1;
		return this.value - sorter.getValue();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeInt(this.value);
	}

	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.value = in.readInt();
	}

	@Override
	public String toString() {
		return name + "\t" + value;
	}
}
