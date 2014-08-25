/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.voole.hobbit2.camus.meta.map.CamusMapperTimeKey;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class CamusMapperTimeKeyTest extends WritableTests {

	@Test
	public void test() throws IOException {
		CamusMapperTimeKey source = new CamusMapperTimeKey();
		source.setTopic("test1");
		source.setCategoryTime(Long.MAX_VALUE);
		source.write(getOutput());
		getOutput().flush();

		CamusMapperTimeKey copy = new CamusMapperTimeKey();
		copy.readFields(getInput());
		Assert.assertEquals(source, copy);
	}
}
