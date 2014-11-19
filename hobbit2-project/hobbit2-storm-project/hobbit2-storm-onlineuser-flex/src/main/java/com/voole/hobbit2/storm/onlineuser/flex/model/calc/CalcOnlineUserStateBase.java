package com.voole.hobbit2.storm.onlineuser.flex.model.calc;

public class CalcOnlineUserStateBase {
	private long total;
	private long low;
	private long stamp;

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getLow() {
		return low;
	}

	public void setLow(long low) {
		this.low = low;
	}

	public long getStamp() {
		return stamp;
	}

	public void setStamp(long stamp) {
		this.stamp = stamp;
	}

	public void append(long total, long low) {
		setLow(getLow() + low);
		setTotal(getTotal() + total);
	}

}
