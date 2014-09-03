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
public interface ICamusTransformer<Source, Target> {
	/**
	 * 一条source可能对应0~n条target,所以在转换后需要判断，是否是多条或0条
	 * 
	 * @param source
	 * @return
	 * @throws CamusTransformException
	 */
	public Optional<Target> transform(Source source)
			throws CamusTransformException;
}
