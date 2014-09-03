/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.voole.hobbit2.camus.api.ICamusKey;
import com.voole.hobbit2.camus.api.TestCamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月3日
 */
public class CamusTransformerManagerTest {
	private static CamusTransformerManager manager;
	private static TestCamusKey key1;
	private static TestCamusKey key2;
	private static TestCamusKey key3;
	private static TestCamusKey vkey;

	@BeforeClass
	public static void before() {
		final Map<ICamusKey, ICamusTransformer<?, ?>> temp = new HashMap<ICamusKey, ICamusTransformer<?, ?>>();
		key1 = new TestCamusKey("test1");
		key2 = new TestCamusKey("test2");
		key3 = new TestCamusKey("test3");
		vkey = new TestCamusKey("testv");

		temp.put(key1, new TestCamusTransformer("_end1"));
		temp.put(key2, new TestCamusTransformer("_end2"));
		temp.put(key3, new ICamusTransformer<String, String>() {

			@Override
			public Optional<String> transform(String source)
					throws CamusTransformException {
				throw new CamusTransformException("kkkk");
			}
		});
		manager = new CamusTransformerManager(new ICamusTransformerRegister() {
			@Override
			public void add(Map<ICamusKey, ICamusTransformer<?, ?>> targetMap) {
				targetMap.putAll(temp);
			}
		});
	}

	@Test
	public void test1() throws CloneNotSupportedException,
			CamusTransformException {
		ICamusTransformer<String, String> t = manager.findTransformer(key1
				.clone());
		Optional<String> r = t.transform("t");
		Assert.assertTrue(r.isPresent());
		Assert.assertEquals("t_end1", r.get());

		t = manager.findTransformer(key2.clone());
		r = t.transform("t");
		Assert.assertTrue(r.isPresent());
		Assert.assertEquals("t_end2", r.get());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void test2() throws CloneNotSupportedException {
		manager.findTransformer(vkey.clone());
	}

	@Test(expected = CamusTransformException.class)
	public void test3() throws CloneNotSupportedException,
			CamusTransformException {
		ICamusTransformer<String, String> t = manager.findTransformer(key3
				.clone());
		t.transform("sjdlfjl");
	}
}
