package com.voole.hobbit2.storm.onlineuser.flex.model.calc;

import com.google.common.base.Objects;

public class PhoenixOnlineUserState {
	private long oemid;
	private long total;
	private long low;

	public long getOemid() {
		return oemid;
	}

	public void setOemid(long oemid) {
		this.oemid = oemid;
	}

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

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("oemid", getOemid())
				.add("total", getTotal()).add("low", getLow()).toString();
	}
}
