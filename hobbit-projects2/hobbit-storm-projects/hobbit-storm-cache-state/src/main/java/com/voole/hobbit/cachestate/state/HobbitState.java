/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import java.util.List;

import storm.trident.state.State;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface HobbitState extends State {
	public void refreshDelay(List<String> cmds);

	public void refreshImmediately(List<String> cmds);

}
