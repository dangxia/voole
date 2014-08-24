package com.voole.hobbit2.camus.meta.category;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CamusMapperTimeCategory implements
		CamusMapperCategory<CamusMapperTimeCategory> {
	private long categoryTime;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(categoryTime);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.categoryTime = in.readLong();

	}

	@Override
	public int compareTo(CamusMapperTimeCategory o) {
		long thisValue = this.categoryTime;
		long thatValue = o.categoryTime;
		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
	}

	public long getCategoryTime() {
		return categoryTime;
	}

	public void setCategoryTime(long categoryTime) {
		this.categoryTime = categoryTime;
	}

}
