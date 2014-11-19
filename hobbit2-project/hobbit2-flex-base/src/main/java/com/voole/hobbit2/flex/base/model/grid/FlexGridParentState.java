package com.voole.hobbit2.flex.base.model.grid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.voole.hobbit2.flex.base.model.FlexBaseState;

public class FlexGridParentState<State extends FlexBaseState, Child extends FlexGridChildState<State>>
		implements Serializable, FlexGridHasState<State> {

	private static final long serialVersionUID = -5009226575685504387L;
	private State state;
	private List<Child> children;

	public FlexGridParentState() {
		children = new ArrayList<Child>();
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public List<Child> getChildren() {
		return children;
	}

	public void setChildren(List<Child> children) {
		this.children = children;
	}
	
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
