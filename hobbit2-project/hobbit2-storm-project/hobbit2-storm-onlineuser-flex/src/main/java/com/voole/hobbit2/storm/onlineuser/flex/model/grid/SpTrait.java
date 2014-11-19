package com.voole.hobbit2.storm.onlineuser.flex.model.grid;

import com.voole.hobbit2.flex.base.model.ParentTrait;

public class SpTrait implements ParentTrait {
	private String spid;

	public SpTrait() {
	}

	public SpTrait(String spid) {
		this.spid = spid;
	}

	public String getSpid() {
		return spid;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

}
