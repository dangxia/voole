package com.voole.hobbit2.storm.onlineuser.flex.model.calc;

public class CalcOemOnlineUserState extends CalcOnlineUserStateBase {
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
