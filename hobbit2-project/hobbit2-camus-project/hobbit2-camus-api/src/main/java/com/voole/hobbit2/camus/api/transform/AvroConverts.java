/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.api.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;

import com.google.common.collect.ImmutableMap;
import com.voole.hobbit2.common.convert.ConvertException;
import com.voole.hobbit2.common.convert.Converts;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class AvroConverts {

	public static Map<Type, Class<?>> avroTypeToJavaClass = new HashMap<Type, Class<?>>();
	static {
		avroTypeToJavaClass.put(Type.INT, Integer.class);
		avroTypeToJavaClass.put(Type.BOOLEAN, Boolean.class);
		avroTypeToJavaClass.put(Type.DOUBLE, Double.class);
		avroTypeToJavaClass.put(Type.ENUM, Enum.class);
		avroTypeToJavaClass.put(Type.FLOAT, Float.class);
		avroTypeToJavaClass.put(Type.LONG, Long.class);
		avroTypeToJavaClass.put(Type.STRING, String.class);
		avroTypeToJavaClass = ImmutableMap.copyOf(avroTypeToJavaClass);
	}

	public static Object convert(Type type, String item)
			throws UnsupportedOperationException, ConvertException {
		return Converts.convert(avroTypeToJavaClass.get(type), item);
	}

}
