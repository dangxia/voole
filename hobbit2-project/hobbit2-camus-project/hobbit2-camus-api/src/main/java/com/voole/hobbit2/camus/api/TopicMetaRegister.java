/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api;

import java.util.List;

import com.voole.hobbit2.camus.api.transform.TransformException;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public interface TopicMetaRegister {
	List<TopicMeta> getTopicMetas() throws TransformException;
}
