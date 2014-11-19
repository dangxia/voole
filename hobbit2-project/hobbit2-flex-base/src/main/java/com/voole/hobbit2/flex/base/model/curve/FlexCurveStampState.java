package com.voole.hobbit2.flex.base.model.curve;

import java.io.Serializable;
import java.util.Date;

import com.voole.hobbit2.flex.base.model.FlexBaseState;

public class FlexCurveStampState<State extends FlexBaseState> implements
		Serializable {
	private static final long serialVersionUID = -4299607646588074930L;
	private Date stamp;
	private State state;

	public Date getStamp() {
		return stamp;
	}

	public void setStamp(Date stamp) {
		this.stamp = stamp;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

}
