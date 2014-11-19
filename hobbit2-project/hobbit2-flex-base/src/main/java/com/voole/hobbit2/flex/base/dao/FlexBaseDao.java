package com.voole.hobbit2.flex.base.dao;

import java.util.Date;
import java.util.List;

import com.voole.hobbit2.flex.base.model.ChildTrait;
import com.voole.hobbit2.flex.base.model.FlexBaseState;
import com.voole.hobbit2.flex.base.model.ParentTrait;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridChildState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridParentState;

public interface FlexBaseDao<State extends FlexBaseState, GridChildState extends FlexGridChildState<State>, GridParentState extends FlexGridParentState<State, GridChildState>, CurveStampState extends FlexCurveStampState<State>, PT extends ParentTrait, CT extends ChildTrait> {
	List<GridParentState> getGridData(String spid, List<String> spids);

	List<CurveStampState> getTotalCurveStampStates(String spid,
			List<String> spids, Date stamp);

	List<CurveStampState> getParentCurveStampStates(PT parentTrait, Date stamp);

	List<CurveStampState> getChildCurveStampStates(CT childTrait, Date stamp);
}
