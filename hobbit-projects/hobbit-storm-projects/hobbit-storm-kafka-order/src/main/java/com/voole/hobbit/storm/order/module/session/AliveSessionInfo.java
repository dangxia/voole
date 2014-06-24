/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.session;

import com.voole.hobbit.cachestate.entity.AreaInfo;
import com.voole.hobbit.cachestate.entity.ResourceInfo;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.storm.order.module.extra.PlayBgnExtra;
import com.voole.hobbit.utils.ProductUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class AliveSessionInfo implements SessionInfo {
	private Long playAlive;// 最后一次心跳时间
	private String spid;
	private Integer areaid;
	private Integer nettype;
	private Integer bitrate;
	private final PlayBgnExtra extra;
	private boolean isVip;
	private long avgSpeed;
	private boolean isLow;

	public AliveSessionInfo(PlayBgnExtra extra) {
		this.extra = extra;
		isVip = false;
		isLow = false;
	}

	@Override
	public String getSessionId() {
		return getExtra().getSessionId();
	}

	@Override
	public Long lastStamp() {
		if (playAlive != null) {
			return playAlive;
		}
		return getExtra().getPlayBgn();
	}

	@Override
	public boolean isDead() {
		return false;
	}

	public Long getPlayAlive() {
		return playAlive;
	}

	public void setPlayAlive(Long playAlive) {
		this.playAlive = playAlive;
	}

	public String getSpid() {
		return spid;
	}

	public void setSpid(String spid) {
		this.spid = spid;
	}

	public Integer getAreaid() {
		return areaid;
	}

	public void setAreaid(Integer areaid) {
		this.areaid = areaid;
	}

	public Integer getNettype() {
		return nettype;
	}

	public void setNettype(Integer nettype) {
		this.nettype = nettype;
	}

	public Integer getBitrate() {
		return bitrate;
	}

	public void setBitrate(Integer bitrate) {
		this.bitrate = bitrate;
	}

	public PlayBgnExtra getExtra() {
		return extra;
	}

	public boolean isVip() {
		return isVip;
	}

	public void setVip(boolean isVip) {
		this.isVip = isVip;
	}

	public long getAvgSpeed() {
		return avgSpeed;
	}

	public void setAvgSpeed(long avgSpeed) {
		this.avgSpeed = avgSpeed;
	}

	public boolean isLow() {
		return isLow;
	}

	public void setLow(boolean isLow) {
		this.isLow = isLow;
	}

	public void update(OrderPlayAliveReqV2 v) {
		setAvgSpeed(v.getSessAvgSpeed());
		setPlayAlive(v.getAliveTick());
		setLow(calcLow());
	}

	public void update(OrderPlayAliveReqV3 v) {
		setAvgSpeed(v.getSessAvgSpeed());
		setPlayAlive(v.getAliveTick());
		setLow(calcLow());
	}

	public boolean calcLow() {
		if (getBitrate() == null || getAvgSpeed() == 0) {
			return false;
		}
		if (getBitrate() * 1024 > getAvgSpeed() * 8) {
			return true;
		}
		return false;
	}

	public static AliveSessionInfo create(PlayBgnExtra extra,
			String spid, AreaInfo areaInfo, ResourceInfo resourceInfo) {
		AliveSessionInfo sessionInfo = new AliveSessionInfo(extra);
		sessionInfo.setSpid(spid);
		if (spid != null && spid.equals(ProductUtils.VOOLE_SPID.toString())
				&& extra.getProductId() != null
				&& extra.getProductId().length() > 0) {
			sessionInfo.setVip(true);
		}
		if (areaInfo != null) {
			sessionInfo.setAreaid(areaInfo.getAreaid());
			sessionInfo.setNettype(areaInfo.getNettype());
		}
		if (resourceInfo != null) {
			sessionInfo.setBitrate(resourceInfo.getBitrate());
		}
		return sessionInfo;
	}
}
