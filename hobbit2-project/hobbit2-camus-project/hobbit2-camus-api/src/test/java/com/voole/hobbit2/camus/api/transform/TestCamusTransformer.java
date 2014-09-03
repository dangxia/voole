/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import com.google.common.base.Optional;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class TestCamusTransformer implements ICamusTransformer<String, String> {
	private final String end;

	public TestCamusTransformer(String end) {
		this.end = end;
	}

	@Override
	public Optional<String> transform(String source)
			throws CamusTransformException {
		return Optional.of(source + end);
	}

}
