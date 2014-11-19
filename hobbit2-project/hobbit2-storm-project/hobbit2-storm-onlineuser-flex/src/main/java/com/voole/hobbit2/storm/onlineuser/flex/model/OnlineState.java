package com.voole.hobbit2.storm.onlineuser.flex.model;

import java.text.DecimalFormat;

import com.google.common.base.Objects;
import com.voole.hobbit2.flex.base.model.FlexBaseState;

public class OnlineState extends FlexBaseState {
	static DecimalFormat format = new DecimalFormat("0.0");
	private static final long serialVersionUID = -5046612851377854459L;
	private long userNum;
	private long lowspeedUserNum;
	private String lowspeedUserPercentage;

	public OnlineState() {
		lowspeedUserPercentage = "0%";
	}

	public long getUserNum() {
		return userNum;
	}

	public void setUserNum(long userNum) {
		this.userNum = userNum;
	}

	public long getLowspeedUserNum() {
		return lowspeedUserNum;
	}

	public void setLowspeedUserNum(long lowspeedUserNum) {
		this.lowspeedUserNum = lowspeedUserNum;
	}

	public String getLowspeedUserPercentage() {
		return lowspeedUserPercentage;
	}

	public void setLowspeedUserPercentage(String lowspeedUserPercentage) {
		this.lowspeedUserPercentage = lowspeedUserPercentage;
	}

	public void calcPercentage() {
		if (getUserNum() == 0) {
			return;
		}
		setLowspeedUserPercentage(format.format(getLowspeedUserNum() * 100.0
				/ getUserNum())
				+ "%");
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("total", getUserNum())
				.add("low", getLowspeedUserNum())
				.add("low_percentage", getLowspeedUserPercentage()).toString();
	}

}
