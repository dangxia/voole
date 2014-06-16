/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module;

import com.voole.hobbit.cachestate.entity.AreaInfo;
import com.voole.hobbit.cachestate.entity.ResourceInfo;
import com.voole.hobbit.storm.order.module.extra.OrderPlayBgnExtra;
import com.voole.hobbit.utils.ProductUtils;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public class OrderBgnSessionInfo implements OrderSessionInfo {
	private Long playAlive;// 最后一次心跳时间
	private String spid;
	private Integer areaid;
	private Integer nettype;
	private Integer bitrate;
	private final OrderPlayBgnExtra extra;
	private boolean isVip;

	public OrderBgnSessionInfo(OrderPlayBgnExtra extra) {
		this.extra = extra;
		isVip = false;
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

	public OrderPlayBgnExtra getExtra() {
		return extra;
	}

	public boolean isVip() {
		return isVip;
	}

	public void setVip(boolean isVip) {
		this.isVip = isVip;
	}

	public static OrderBgnSessionInfo create(OrderPlayBgnExtra extra,
			String spid, AreaInfo areaInfo, ResourceInfo resourceInfo) {
		OrderBgnSessionInfo sessionInfo = new OrderBgnSessionInfo(extra);
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
