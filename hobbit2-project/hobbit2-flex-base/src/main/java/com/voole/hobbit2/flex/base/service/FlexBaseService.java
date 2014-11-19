package com.voole.hobbit2.flex.base.service;

import java.util.Date;
import java.util.List;

import com.voole.hobbit2.flex.base.model.ChildTrait;
import com.voole.hobbit2.flex.base.model.FlexBaseState;
import com.voole.hobbit2.flex.base.model.ParentTrait;
import com.voole.hobbit2.flex.base.model.curve.FlexChartData;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridChildState;
import com.voole.hobbit2.flex.base.model.grid.FlexGridParentState;

public interface FlexBaseService<State extends FlexBaseState, GridChildState extends FlexGridChildState<State>, GridParentState extends FlexGridParentState<State, GridChildState>, CurveStampState extends FlexCurveStampState<State>, PT extends ParentTrait, CT extends ChildTrait> {
	List<GridParentState> getGridData(String spid);

	FlexChartData<CurveStampState> getTotalFlexChartData(String spid, Date stamp);

	FlexChartData<CurveStampState> getParentFlexChartData(PT parentTrait,
			Date stamp);

	FlexChartData<CurveStampState> getChildFlexChartData(CT childTrait,
			Date stamp);
}
