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

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.voole.hobbit.exception.TransformException;

/**
 * @author XuehuiHe
 * @date 2014年6月3日
 */
public class TransformerUtil {
	public static final SimpleDateFormat sf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static final Map<JavaType, Class<?>> typeToClass = new HashMap<JavaType, Class<?>>();
	static {
		typeToClass.put(JavaType.INT, Integer.class);
		typeToClass.put(JavaType.BOOLEAN, Boolean.class);
		typeToClass.put(JavaType.DOUBLE, Double.class);
		typeToClass.put(JavaType.ENUM, Enum.class);
		typeToClass.put(JavaType.FLOAT, Float.class);
		typeToClass.put(JavaType.LONG, Long.class);
		typeToClass.put(JavaType.STRING, String.class);
	}

	public static Object get(JavaType type, String item)
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
			TransformException ex = new TransformException(e);
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
