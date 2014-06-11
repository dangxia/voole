/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.state;

import java.util.List;

import storm.trident.state.map.ReadOnlyMapState;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public interface TimeoutState<T extends TimeoutAbled> extends
		ReadOnlyMapState<T> {
	long getMaxTimeoutedStamp();

	public List<T> timeout();

	List<T> multiPut(List<List<Object>> keys, List<T> vals);

}
