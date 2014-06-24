/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.session;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface SessionInfo extends Serializable {
	public String getSessionId();

	public Long lastStamp();

	public boolean isDead();
}
