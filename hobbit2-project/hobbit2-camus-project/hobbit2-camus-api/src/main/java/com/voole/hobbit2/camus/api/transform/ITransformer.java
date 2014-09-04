/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import com.google.common.base.Optional;

/**
 * 
 * Camus转换器
 * 
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public interface ITransformer<Source, Target> {
	/**
	 * 一条source可能对应0~1条target,所以在转换后需要判断
	 * 
	 * @param source
	 * @return
	 * @throws TransformException
	 */
	public Optional<Target> transform(Source source)
			throws TransformException;
}
