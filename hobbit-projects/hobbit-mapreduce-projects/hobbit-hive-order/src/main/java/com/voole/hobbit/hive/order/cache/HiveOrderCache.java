/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.cache;

import com.voole.hobbit.avro.hive.HiveOrderRecord;
import com.voole.hobbit.cache.AreaInfoCache;
import com.voole.hobbit.cache.OemInfoCache;
import com.voole.hobbit.cache.ResourceInfoCache;

/**
 * @author XuehuiHe
 * @date 2014年7月31日
 */
public interface HiveOrderCache extends AreaInfoCache, OemInfoCache,
		ResourceInfoCache {
	void deal(HiveOrderRecord record);

	public void open();

	public void close();
}
