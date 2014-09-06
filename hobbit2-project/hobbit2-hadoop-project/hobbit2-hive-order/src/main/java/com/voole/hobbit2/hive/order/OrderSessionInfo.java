/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order;

import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;
import com.voole.hobbit2.hive.order.exception.OrderSessionInfoBgnNullException;
import com.voole.hobbit2.hive.order.exception.OrderSessionInfoException;

/**
 * 该点播session处理的前提为sessionID唯一，sessionID不唯一结果可能有一定出入
 * 
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class OrderSessionInfo {
	private String sessionId;
	public Object _bgn;
	public long _bgnTime;

	public Object _end;
	public long _endTime;

	public Object _lastAlive;
	public long _lastAliveTime;

	public void clear() {
		this.sessionId = null;
		_bgn = null;
		_bgnTime = 0l;

		_end = null;
		_endTime = 0l;

		_lastAlive = null;
		_lastAliveTime = 0l;
	}

	public void verify() throws OrderSessionInfoException {
		if (_bgn == null) {
			throw new OrderSessionInfoBgnNullException(sessionId, "bgn is null");
		}
		if (_end != null && _bgnTime > _endTime) {
			throw new OrderSessionInfoException(getSessionId(),
					"_bgnTime > _endTime");
		}
		if (_lastAlive != null && _bgnTime > _lastAliveTime) {
			throw new OrderSessionInfoException(getSessionId(),
					"_bgnTime > _lastAliveTime");
		}

		if (_lastAlive != null && _end != null && _endTime < _lastAliveTime) {
			throw new OrderSessionInfoException(getSessionId(),
					"_endTime < _lastAliveTime");
		}
	}

	public void setBgn(OrderPlayBgnReqV2 bgn) throws OrderSessionInfoException {
		setBgn(bgn, bgn.getPlayTick());
	}

	public void setBgn(OrderPlayBgnReqV3 bgn) throws OrderSessionInfoException {
		setBgn(bgn, bgn.getPlayTick());
	}

	public void setBgn(Object bgn, Long bgnTime)
			throws OrderSessionInfoException {
		if (_bgn != null) {
			throw new OrderSessionInfoException(getSessionId(), "bgn is multi!");
		}
		_bgn = bgn;
		_bgnTime = bgnTime;
		// if (_bgn == null || bgnTime > _bgnTime) {
		// _bgn = bgn;
		// _bgnTime = bgnTime;
		// }
	}

	public void setEnd(OrderPlayEndReqV2 end) throws OrderSessionInfoException {
		setEnd(end, end.getEndTick());
	}

	public void setEnd(OrderPlayEndReqV3 end) throws OrderSessionInfoException {
		setEnd(end, end.getEndTick());
	}

	public void setEnd(Object end, Long endTime)
			throws OrderSessionInfoException {
		if (_end != null) {
			throw new OrderSessionInfoException(getSessionId(), "end is multi!");
		}
		_end = end;
		_endTime = endTime;
		// if (_end == null || endTime > _endTime) {
		// _end = end;
		// _endTime = endTime;
		// }
	}

	public void setAlive(OrderPlayAliveReqV2 alive) {
		setAlive(alive, alive.getAliveTick());
	}

	public void setAlive(OrderPlayAliveReqV3 alive) {
		setAlive(alive, alive.getAliveTick());
	}

	public void setAlive(Object alive, Long aliveTime) {
		if (_lastAlive == null || aliveTime > _lastAliveTime) {
			_lastAlive = alive;
			_lastAliveTime = aliveTime;
		}
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

}