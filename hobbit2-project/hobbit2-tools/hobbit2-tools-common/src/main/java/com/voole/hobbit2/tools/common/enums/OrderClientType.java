/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common.enums;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public enum OrderClientType {
	RTSP(0), PC(1), TV(2), PHONE(3);
	private final int intValue;

	private OrderClientType(int intValue) {
		this.intValue = intValue;
	}

	public int getIntValue() {
		return intValue;
	}

	public static OrderClientType getOrderClientType(Long oemid) {
		// 协议
		if (oemid == null) {
		} else if (oemid == 100) {// pc客户端官方版
			return OrderClientType.PC;
		} else if (oemid > 100 && oemid < 500) {// 互联网电视
			return OrderClientType.TV;
		} else if (oemid >= 500 && oemid < 800) {// PC客户端
			return OrderClientType.PC;
		} else if (oemid >= 800 && oemid < 999) {// 手机客户端
			return OrderClientType.PHONE;
		}
		return OrderClientType.RTSP;
	}
}
