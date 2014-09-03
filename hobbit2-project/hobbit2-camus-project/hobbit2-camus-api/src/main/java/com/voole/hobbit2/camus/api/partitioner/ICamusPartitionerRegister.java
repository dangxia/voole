/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.partitioner;

import java.util.Map;

import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public interface ICamusPartitionerRegister {

	/**
	 * @param tempPartitionerMap
	 */
	public void add(Map<ICamusKey, ICamusPartitioner<?, ?>> tempPartitionerMap);
}
