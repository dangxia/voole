/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.Map;

import com.voole.hobbit2.camus.api.ICamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class TestCamusTransformerRegister implements
		ICamusTransformerRegister {
	Map<ICamusKey, ICamusTransformer<?, ?>> temp;

	public TestCamusTransformerRegister(
			Map<ICamusKey, ICamusTransformer<?, ?>> temp) {
		this.temp = temp;
	}

	@Override
	public void add(Map<ICamusKey, ICamusTransformer<?, ?>> targetMap) {
		targetMap.putAll(temp);
	}

}
