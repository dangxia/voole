package com.voole.hobbit.storm.order.module.session;

import java.io.Serializable;

import com.voole.hobbit.storm.order.module.extra.PlayExtra.PlayType;
import com.voole.hobbit.utils.HobbitUtils;

public class SessionTick implements Serializable {
	private PlayType type;
	private String spid;
	private Long oemid;

	private String sessionId;
	private Long lastStamp;

	private boolean isLow;
	private boolean isVip;

	private String hid;

	public SessionTick(PlayType type, String hid) {
		this.type = type;
		this.hid = hid;
	}

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

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public Long getLastStamp() {
		return lastStamp;
	}

	public void setLastStamp(Long lastStamp) {
		this.lastStamp = lastStamp;
	}

	public boolean isLow() {
		return isLow;
	}

	public void setLow(boolean isLow) {
		this.isLow = isLow;
	}

	public boolean isVip() {
		return isVip;
	}

	public void setVip(boolean isVip) {
		this.isVip = isVip;
	}

	public PlayType getType() {
		return type;
	}

	public void setType(PlayType type) {
		this.type = type;
	}

	public String getHid() {
		return hid;
	}

	public void setHid(String hid) {
		this.hid = hid;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (obj instanceof SessionTick) {
			SessionTick that = (SessionTick) obj;
			return HobbitUtils.equals(this.getOemid(), that.getOemid())
					&& HobbitUtils.equals(this.getSpid(), that.getSpid())
					&& this.isLow() == that.isLow()
					&& this.isVip() == that.isVip();
		}
		return false;
	}

}
