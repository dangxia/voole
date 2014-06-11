/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年10月28日
 */
public class IpRange implements Serializable {
	private static final long serialVersionUID = 6702698891221476064L;
	private Long minip;
	private Long maxip;
	private Integer areaid;
	private Integer nettype;

	public IpRange() {
	}

	public Long getMinip() {
		return minip;
	}

	public Long getMaxip() {
		return maxip;
	}

	public Integer getAreaid() {
		return areaid;
	}

	public Integer getNettype() {
		return nettype;
	}

	public void setMinip(Long minip) {
		this.minip = minip;
	}

	public void setMaxip(Long maxip) {
		this.maxip = maxip;
	}

	public void setAreaid(Integer areaid) {
		this.areaid = areaid;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

}
