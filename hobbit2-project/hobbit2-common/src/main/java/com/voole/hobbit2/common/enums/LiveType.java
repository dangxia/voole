/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.common.enums;

/**
 * 
 * 直播类型
 * 
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public enum LiveType {
	LIVE(0), REPEAT(1);
	private final int intValue;

	private LiveType(int intValue) {
		this.intValue = intValue;
	}

	public int getIntValue() {
		return intValue;
	}

	public static LiveType getLiveType(int localChannelId) {
		if (localChannelId > 600 && localChannelId <= 699) {
			return REPEAT;
		} else {
			return LIVE;
		}
	}
}
