/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.util;

import static com.voole.hobbit.util.TopicUtils.ORDER_ALIVE_V2;
import static com.voole.hobbit.util.TopicUtils.ORDER_ALIVE_V3;
import static com.voole.hobbit.util.TopicUtils.ORDER_BGN_V2;
import static com.voole.hobbit.util.TopicUtils.ORDER_BGN_V3;
import static com.voole.hobbit.util.TopicUtils.ORDER_END_V2;
import static com.voole.hobbit.util.TopicUtils.ORDER_END_V3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit.avro.termial.OrderPlayAliveReqV2;
import com.voole.hobbit.avro.termial.OrderPlayAliveReqV3;
import com.voole.hobbit.avro.termial.OrderPlayBgnReqV2;
import com.voole.hobbit.avro.termial.OrderPlayBgnReqV3;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV2;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV3;
import com.voole.hobbit.transformer.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年7月10日
 */
public class AvroUtils {
	public static final SimpleDateFormat sf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static final Map<Type, Class<?>> avroTypeToJavaClass = new HashMap<Type, Class<?>>();
	static {
		avroTypeToJavaClass.put(Type.INT, Integer.class);
		avroTypeToJavaClass.put(Type.BOOLEAN, Boolean.class);
		avroTypeToJavaClass.put(Type.DOUBLE, Double.class);
		avroTypeToJavaClass.put(Type.ENUM, Enum.class);
		avroTypeToJavaClass.put(Type.FLOAT, Float.class);
		avroTypeToJavaClass.put(Type.LONG, Long.class);
		avroTypeToJavaClass.put(Type.STRING, String.class);
	}

	public static final Map<String, Schema> topicToSchema = new HashMap<String, Schema>();
	static {
		topicToSchema.put(ORDER_BGN_V2, OrderPlayBgnReqV2.getClassSchema());
		topicToSchema.put(ORDER_BGN_V3, OrderPlayBgnReqV3.getClassSchema());

		topicToSchema.put(ORDER_END_V2, OrderPlayEndReqV2.getClassSchema());
		topicToSchema.put(ORDER_END_V3, OrderPlayEndReqV3.getClassSchema());

		topicToSchema.put(ORDER_ALIVE_V2, OrderPlayAliveReqV2.getClassSchema());
		topicToSchema.put(ORDER_ALIVE_V3, OrderPlayAliveReqV3.getClassSchema());
	}

	public static Schema getKafkaTopicSchema(String topic) {
		return topicToSchema.get(topic);
	}

	public static Object get(Type type, String item)
			throws TransformerException {
		return get(avroTypeToJavaClass.get(type), item);
	}

	@SuppressWarnings("unchecked")
	public static Class<? extends SpecificRecordBase> getSchemaClass(
			Schema schema) throws ClassNotFoundException {
		return ((Class<? extends SpecificRecordBase>) Class.forName(schema
				.getFullName()));

	}

	public static Object get(Class<?> clazz, String b)
			throws TransformerException {
		if (b == null) {
			return null;
		}
		b = b.trim();
		if (!clazz.equals(String.class) && "".equals(b)) {
			return null;
		}
		try {
			if (clazz.equals(boolean.class) || clazz.equals(Boolean.class)) {
				return new Boolean(b);
			} else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
				return new Long(b);
			} else if (clazz.equals(Short.class) || clazz.equals(short.class)) {
				return new Short(b);
			} else if (clazz.equals(Double.class) || clazz.equals(double.class)) {
				return new Double(b);
			} else if (clazz.equals(Float.class) || clazz.equals(float.class)) {
				return new Float(b);
			} else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
				return new Integer(b);
			} else if (clazz.equals(String.class)) {
				return b;
			} else if (clazz.equals(BigDecimal.class)) {
				return new BigDecimal(b);
			} else if (Date.class.isAssignableFrom(clazz)) {
				return getDate(b);
			} else if (clazz.equals(BigInteger.class)) {
				return new BigInteger(b);
			}
		} catch (Exception e) {
			KafkaTransformException ex = new KafkaTransformException(e);
			ex.setTarget(clazz);
			ex.setSource(b);
			throw ex;
		}

		return null;
	}

	public static Date getDate(String d) throws ParseException {
		if (d.matches("^\\d+$")) {
			Long time = new Long(d);
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(time);
			return c.getTime();
		} else {
			return sf.parse("2" + d);
		}
	}
}
