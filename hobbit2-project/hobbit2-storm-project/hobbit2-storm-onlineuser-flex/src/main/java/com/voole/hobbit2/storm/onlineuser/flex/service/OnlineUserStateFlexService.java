package com.voole.hobbit2.storm.onlineuser.flex.service;

import com.voole.hobbit2.flex.base.model.curve.FlexCurveStampState;
import com.voole.hobbit2.flex.base.service.FlexBaseService;
import com.voole.hobbit2.storm.onlineuser.flex.model.OnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemTrait;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpGridOnlineState;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpTrait;

public interface OnlineUserStateFlexService extends
FlexBaseService<OnlineState, OemGridOnlineState, SpGridOnlineState, FlexCurveStampState<OnlineState>, SpTrait, OemTrait>{

}
