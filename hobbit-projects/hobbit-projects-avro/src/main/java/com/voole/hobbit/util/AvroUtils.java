/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.TransformerException;

import org.apache.avro.Schema.Type;

import com.voole.hobbit.transformer.KafkaTransformException;

/**
 * @author XuehuiHe
 * @date 2014年7月10日
 */
public class AvroUtils {
	public static final SimpleDateFormat sf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static final Map<Type, Class<?>> typeToClass = new HashMap<Type, Class<?>>();
	static {
		typeToClass.put(Type.INT, Integer.class);
		typeToClass.put(Type.BOOLEAN, Boolean.class);
		typeToClass.put(Type.DOUBLE, Double.class);
		typeToClass.put(Type.ENUM, Enum.class);
		typeToClass.put(Type.FLOAT, Float.class);
		typeToClass.put(Type.LONG, Long.class);
		typeToClass.put(Type.STRING, String.class);
	}

	public static Object get(Type type, String item)
			throws TransformerException {
		return get(typeToClass.get(type), item);
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
