/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class OrderOnlineUser implements Serializable {
	private long userNum;
	private long userNum_l;
	private long vipNum;
	private long vipNum_l;

	public long getUserNum() {
		return userNum;
	}

	public void setUserNum(long userNum) {
		this.userNum = userNum;
	}

	public long getUserNum_l() {
		return userNum_l;
	}

	public void setUserNum_l(long userNum_l) {
		this.userNum_l = userNum_l;
	}

	public long getVipNum() {
		return vipNum;
	}

	public void setVipNum(long vipNum) {
		this.vipNum = vipNum;
	}

	public long getVipNum_l() {
		return vipNum_l;
	}

	public void setVipNum_l(long vipNum_l) {
		this.vipNum_l = vipNum_l;
	}

	public void update(OrderOnlineUser modify) {
		setUserNum(getUserNum() + modify.getUserNum());
		setUserNum_l(getUserNum_l() + modify.getUserNum_l());

		setVipNum(getVipNum() + modify.getVipNum());
		setVipNum_l(getVipNum_l() + modify.getVipNum_l());
	}

}
