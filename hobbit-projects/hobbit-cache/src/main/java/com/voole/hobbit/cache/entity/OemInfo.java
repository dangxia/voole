/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年10月25日
 */
public class OemInfo implements Serializable {
	private static final long serialVersionUID = -5512697435208367106L;
	private Long oemid;
	private String repeatPortalid;
	private String livePortalid;
	private String spid;
	private Long tid;
	private Long policyid;
	private String shortname;

	public OemInfo() {
	}

	public Long getOemid() {
		return oemid;
	}

	public String getRepeatPortalid() {
		return repeatPortalid;
	}

	public String getLivePortalid() {
		return livePortalid;
	}

	public String getSpid() {
		return spid;
	}

	public void setOemid(Long oemid) {
		this.oemid = oemid;
	}

	public void setRepeatPortalid(String repeatPortalid) {
		this.repeatPortalid = repeatPortalid;
	}

	public void setLivePortalid(String livePortalid) {
		this.livePortalid = livePortalid;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

	public Long getTid() {
		return tid;
	}

	public void setTid(Long tid) {
		this.tid = tid;
	}

	public Long getPolicyid() {
		return policyid;
	}

	public void setPolicyid(Long policyid) {
		this.policyid = policyid;
	}

	public String getShortname() {
		return shortname;
	}

	public void setShortname(String shortname) {
		this.shortname = shortname;
	}

}
