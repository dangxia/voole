/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.state;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public interface TimeoutAbled extends Serializable {
	long getStamp();
}
