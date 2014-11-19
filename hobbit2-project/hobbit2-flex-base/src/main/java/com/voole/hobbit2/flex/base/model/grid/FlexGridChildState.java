package com.voole.hobbit2.flex.base.model.grid;

import java.io.Serializable;

import com.voole.hobbit2.flex.base.model.FlexBaseState;

public class FlexGridChildState<State extends FlexBaseState> implements
		Serializable, FlexGridHasState<State> {
	private static final long serialVersionUID = 5281773840752670498L;
	private State state;

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
	
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
