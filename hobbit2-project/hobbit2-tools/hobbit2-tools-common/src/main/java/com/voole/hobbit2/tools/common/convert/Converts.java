/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common.convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author XuehuiHe
 * @date 2014年8月22日
 */
public class Converts {
	public static final SimpleDateFormat sf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings("unchecked")
	public static <T> T convert(Class<T> clazz, String b)
			throws ConvertException, ConvertDontSupportException {
		if (b == null) {
			return null;
		}
		b = b.trim();
		if (!clazz.equals(String.class) && "".equals(b)) {
			return null;
		}
		try {
			if (clazz.equals(boolean.class) || clazz.equals(Boolean.class)) {
				return (T) new Boolean(b);
			} else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
				return (T) Long.valueOf(b);
			} else if (clazz.equals(Short.class) || clazz.equals(short.class)) {
				return (T) Short.valueOf(b);
			} else if (clazz.equals(Double.class) || clazz.equals(double.class)) {
				return (T) Double.valueOf(b);
			} else if (clazz.equals(Float.class) || clazz.equals(float.class)) {
				return (T) Float.valueOf(b);
			} else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
				return (T) Integer.valueOf(b);
			} else if (clazz.equals(String.class)) {
				return (T) b;
			} else if (clazz.equals(BigDecimal.class)) {
				return (T) new BigDecimal(b);
			} else if (clazz.equals(BigInteger.class)) {
				return (T) new BigInteger(b);
			} else if (Date.class.isAssignableFrom(clazz)) {
				return (T) getDate(b);
			}
		} catch (Exception e) {
			ConvertException ex = new ConvertException(e);
			ex.setTarget(clazz);
			ex.setSource(b);
			throw ex;
		}

		throw new ConvertDontSupportException("target clazz:" + clazz.getName()
				+ "don't support!");
	}

	public static Date getDate(String d) throws ParseException {
		if (d.matches("^\\d+$")) {
			Long time = new Long(d);
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(time);
			return c.getTime();
		} else {
			return sf.parse(d);
		}
	}
}
