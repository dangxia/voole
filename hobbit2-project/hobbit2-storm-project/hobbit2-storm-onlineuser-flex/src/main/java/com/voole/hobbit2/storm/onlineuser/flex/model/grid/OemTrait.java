package com.voole.hobbit2.storm.onlineuser.flex.model.grid;

import com.voole.hobbit2.flex.base.model.ChildTrait;

public class OemTrait implements ChildTrait {
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

}
