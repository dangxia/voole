/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.util;

import com.voole.hobbit2.camus.api.TopicMetaManager;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.storm.order.StormOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TopicMetaManagerUtil {

	public static TopicMetaManager get() throws TransformException {
		return StormOrderMetaConfigs.getTopicMetaManager();
	}

}
