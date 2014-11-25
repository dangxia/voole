package com.voole.hobbit2.storm.onlineuser.flex.dao;

import java.util.List;

import com.voole.hobbit2.flex.base.dao.FlexBaseDao;
import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcOemOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.CalcSpOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.calc.PhoenixOnlineUserState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemTrait;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpTrait;

public interface PhoenixDao
		extends
		FlexBaseDao<OnlineState, OemGridOnlineState, SpGridOnlineState, FlexCurveStampState<OnlineState>, SpTrait, OemTrait> {
	List<PhoenixOnlineUserState> queryOnlineUserState();

	void updateOnlineUserState(
			List<CalcOemOnlineUserState> oemOnlineUserStates,
			List<CalcSpOnlineUserState> spOnlineUserStates);

	void sync();
}
