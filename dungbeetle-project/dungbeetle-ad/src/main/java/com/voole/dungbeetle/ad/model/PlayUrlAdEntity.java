package com.voole.dungbeetle.ad.model;

import java.util.List;

/**
 * 播放url中广告信息
 */
public class PlayUrlAdEntity {
	private String fid;//广告介质id
	private Integer startTimeLength;//开始时间
	private Integer playTimeLength;//播放时长
	private String adSize;//广告大小
	private String pid;
	private String po;
	private String mid;//playurl解出 影片节目id
	
	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	private String admergetype;//广告merge类型
	private String logid;//投放日志id
	private List<String> plnidList; //广告排期id列表

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPo() {
		return po;
	}

	public void setPo(String po) {
		this.po = po;
	}

	public String getFid() {
		return fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
	}

	public Integer getStartTimeLength() {
		return startTimeLength;
	}

	public void setStartTimeLength(Integer startTimeLength) {
		this.startTimeLength = startTimeLength;
	}

	public Integer getPlayTimeLength() {
		return playTimeLength;
	}

	public void setPlayTimeLength(Integer playTimeLength) {
		this.playTimeLength = playTimeLength;
	}

	public String getAdSize() {
		return adSize;
	}

	public void setAdSize(String adSize) {
		this.adSize = adSize;
	}

	public String getAdmergetype() {
		return admergetype;
	}

	public void setAdmergetype(String admergetype) {
		this.admergetype = admergetype;
	}

	public String getLogid() {
		return logid;
	}

	public void setLogid(String logid) {
		this.logid = logid;
	}

	public List<String> getPlnidList() {
		return plnidList;
	}

	public void setPlnidList(List<String> plnidList) {
		this.plnidList = plnidList;
	}
	
	
}