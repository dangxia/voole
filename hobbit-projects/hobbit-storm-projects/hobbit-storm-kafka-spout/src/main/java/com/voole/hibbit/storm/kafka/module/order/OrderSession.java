/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.module.order;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public abstract class OrderSession implements Serializable {
	private String sessID;
	private long bgn;
	private long tick;
	private long end;

	public long getTick() {
		return tick;
	}

	public void setTick(long tick) {
		this.tick = tick;
	}

	public String getSessID() {
		return sessID;
	}

	public void setSessID(String sessID) {
		this.sessID = sessID;
	}

	public long getBgn() {
		return bgn;
	}

	public void setBgn(long bgn) {
		this.bgn = bgn;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

}
