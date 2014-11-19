package com.voole.hobbit2.flex.base.model.grid;

import com.voole.hobbit2.flex.base.model.FlexBaseState;

public interface FlexGridHasState<State extends FlexBaseState> {
	public State getState();
}
