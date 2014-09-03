/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class CamusTransformerTest {

	public static List<String> strs = Lists.newArrayList("sdf", "sjdlfjsl",
			"sjdlfjsldjf", "sjdlfj");

	@Test
	public void testEmptyTarget() throws CamusTransformException {
		ICamusTransformer<String, String> transformer = new ICamusTransformer<String, String>() {

			@Override
			public Optional<String> transform(String source)
					throws CamusTransformException {
				return Optional.absent();
			}
		};
		Optional<String> result = transformer.transform("sjdlfj");
		Assert.assertTrue(!result.isPresent());
	}

	@Test
	public void testMultiTarget() throws CamusTransformException {

		ICamusTransformer<String, Iterable<String>> transformer = new ICamusTransformer<String, Iterable<String>>() {
			@Override
			public Optional<Iterable<String>> transform(String source)
					throws CamusTransformException {
				return Optional.of((Iterable<String>) Lists.newArrayList(source
						.split(",")));
			}
		};

		Optional<Iterable<String>> result = transformer.transform(Joiner
				.on(",").join(strs));
		Assert.assertTrue(result.isPresent());
		int i = 0;
		for (Iterator<String> iterator = result.get().iterator(); iterator
				.hasNext(); i++) {
			Assert.assertEquals(strs.get(i), iterator.next());
		}

	}
}
