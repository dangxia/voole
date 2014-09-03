/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.partitioner;

import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public interface ICamusPartitioner<Key extends ICamusKey, Value> {
	void partition(Key key, Value value);

	String getPath(Key key);
}
