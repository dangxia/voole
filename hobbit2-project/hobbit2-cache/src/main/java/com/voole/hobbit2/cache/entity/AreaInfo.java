/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年10月30日
 */
public class AreaInfo implements Serializable {

	private static final long serialVersionUID = 3829607853380461002L;

	private Integer areaid;
	private Integer nettype;

	public AreaInfo() {
	}

	public AreaInfo(Integer areaid, Integer nettype) {
		this.areaid = areaid;
		this.nettype = nettype;
	}

	public Integer getAreaid() {
		return areaid;
	}

	public Integer getNettype() {
		return nettype;
	}

	public void setAreaid(Integer areaid) {
		this.areaid = areaid;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

}
