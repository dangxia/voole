/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.state;

import java.util.List;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public interface IBackingMapTimeoutState<T extends TimeoutAbled> {
	public List<T> timeout();
}
