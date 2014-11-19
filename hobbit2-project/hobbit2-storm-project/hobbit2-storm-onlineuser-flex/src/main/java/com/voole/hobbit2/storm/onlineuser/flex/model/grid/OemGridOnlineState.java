package com.voole.hobbit2.storm.onlineuser.flex.model.grid;

import com.google.common.base.Objects;
import com.voole.hobbit2.flex.base.model.grid.FlexGridChildState;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;

public class OemGridOnlineState extends FlexGridChildState<OnlineState> {
	private static final long serialVersionUID = 3599298051005498210L;
	private String spid;
	private Long oemid;

	public String getSpid() {
		return spid;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

	public Long getOemid() {
		return oemid;
	}

	public void setOemid(Long oemid) {
		this.oemid = oemid;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("spid", spid)
				.add("name", getName()).add("state", getState()).toString();
	}

}
