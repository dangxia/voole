package com.voole.hobbit2.flex.base.model.curve;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FlexChartData<StampState extends FlexCurveStampState<?>>
		implements Serializable {
	private static final long serialVersionUID = 5374057053792604828L;
	private String name;
	private Date maxDate;
	private List<StampState> data;

	public FlexChartData() {
		data = new ArrayList<StampState>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getMaxDate() {
		return maxDate;
	}

	public void setMaxDate(Date maxDate) {
		this.maxDate = maxDate;
	}

	public List<StampState> getData() {
		return data;
	}

	public void setData(List<StampState> data) {
		this.data = data;
	}

	public void calcMaxDate() {
		Date maxDate = new Date(0);
		for (StampState stampState : data) {
			if (stampState.getStamp().getTime() > maxDate.getTime()) {
				maxDate = stampState.getStamp();
			}
		}
		this.maxDate = maxDate;
	}
}
