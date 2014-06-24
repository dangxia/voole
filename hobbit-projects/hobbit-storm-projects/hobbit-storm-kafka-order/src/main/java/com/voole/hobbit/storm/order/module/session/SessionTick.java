package com.voole.hobbit.storm.order.module.session;

import java.io.Serializable;

public class SessionTick implements Serializable {
	private OrderSessionTickType type;
	private String spid;
	private Long oemid;

	private String sessionId;
	private Long lastStamp;

	private boolean isLow;
	private boolean isVip;

	public SessionTick(OrderSessionTickType type) {
		this.type = type;
	}

	public boolean isAlive() {
		return type == OrderSessionTickType.ALIVE
				|| type == OrderSessionTickType.BGN;
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

	public OrderSessionTickType getType() {
		return type;
	}

	public void setType(OrderSessionTickType type) {
		this.type = type;
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
			return this.getOemid().equals(that.getOemid())
					&& this.getSpid().equals(that.getSpid())
					&& this.isLow() == that.isLow()
					&& this.isVip() == that.isVip();
		}
		return false;
	}

	public static enum OrderSessionTickType {
		BGN, END, ALIVE;
	}
}
