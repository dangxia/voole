/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.Map;

import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * transformer注册器
 * 
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public interface ICamusTransformerRegister {
	public void add(Map<ICamusKey, ICamusTransformer<?, ?>> targetMap);
}
