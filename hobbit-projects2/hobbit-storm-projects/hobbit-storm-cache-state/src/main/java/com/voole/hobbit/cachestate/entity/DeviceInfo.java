/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年10月30日
 */
public class DeviceInfo implements Serializable {
	private static final long serialVersionUID = 3973176837055873988L;
	private String idcid;
	private Integer areaid;
	private Integer nettype;
	private String ltIp;
	private String dxIp;

	public DeviceInfo() {
	}

	public String getIdcid() {
		return idcid;
	}

	public Integer getAreaid() {
		return areaid;
	}

	public Integer getNettype() {
		return nettype;
	}

	public String getLtIp() {
		return ltIp;
	}

	public String getDxIp() {
		return dxIp;
	}

	public void setIdcid(String idcid) {
		this.idcid = idcid;
	}

	public void setAreaid(Integer areaid) {
		this.areaid = areaid;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

	public void setLtIp(String ltIp) {
		this.ltIp = ltIp;
	}

	public void setDxIp(String dxIp) {
		this.dxIp = dxIp;
	}

}
