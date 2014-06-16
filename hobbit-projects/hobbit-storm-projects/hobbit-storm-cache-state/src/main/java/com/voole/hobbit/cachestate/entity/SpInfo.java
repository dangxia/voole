/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年11月15日
 */
public class SpInfo implements Serializable {
	private static final long serialVersionUID = 6329717091076911723L;
	private String spid;
	private String shortname;
	private Integer nettype;

	public SpInfo() {
	}

	public String getSpid() {
		return spid;
	}

	public String getShortname() {
		return shortname;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

	public void setShortname(String shortname) {
		this.shortname = shortname;
	}

	public Integer getNettype() {
		return nettype;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

}
