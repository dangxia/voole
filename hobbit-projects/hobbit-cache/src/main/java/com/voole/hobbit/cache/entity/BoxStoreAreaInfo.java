/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年10月28日
 */
public class BoxStoreAreaInfo implements Serializable {
	private static final long serialVersionUID = 5614889481804350038L;
	private String oemid;
	private String mac;
	private Integer areaid;
	private Integer nettype;

	public BoxStoreAreaInfo() {
	}

	public String getOemid() {
		return oemid;
	}

	public String getMac() {
		return mac;
	}

	public Integer getAreaid() {
		return areaid;
	}

	public Integer getNettype() {
		return nettype;
	}

	public void setOemid(String oemid) {
		this.oemid = oemid;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public void setAreaid(Integer areaid) {
		this.areaid = areaid;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

}
