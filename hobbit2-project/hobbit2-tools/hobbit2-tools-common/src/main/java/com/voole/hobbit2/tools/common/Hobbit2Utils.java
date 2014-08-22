/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author XuehuiHe
 * @date 2014年6月12日
 */
public class Hobbit2Utils {
	private static final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static String longToIp(long i) {
		return ((i >> 24) & 0xFF) + "." + ((i >> 16) & 0xFF) + "."
				+ ((i >> 8) & 0xFF) + "." + (i & 0xFF);
	}

	public static Long ipToLong(String ipAddress) {
		long result = 0;
		String[] ipAddressInArray = ipAddress.split("\\.");
		for (int i = 3; i >= 0; i--) {
			long ip = Long.parseLong(ipAddressInArray[3 - i]);
			result |= ip << (i * 8);

		}
		return result;
	}

	public static long exchangeBigLittle(long i) {
		return (i << 24) | ((i & 0xFF00) << 8) | ((i >> 8) & 0xFF00)
				| (i >> 24);
	}

	public static Date secondLongVToDate(long d) {
		return new Date(d * 1000);
	}

	public static String secondLongVToDateStr(long d) {
		return dateToStr(secondLongVToDate(d));
	}

	public static String dateToStr(Date d) {
		return df.format(d);
	}

	public static boolean equals(Object v1, Object v2) {
		if (v1 == null && v2 == null) {
			return true;
		}
		if (v1 != null) {
			return v1.equals(v2);
		} else {
			return v2.equals(v1);
		}
	}

	public static int hashCode(Object o) {
		if (o == null) {
			return 0;
		}
		return o.hashCode();
	}

	public static String toString(Object o) {
		if (o == null) {
			return "null";
		}
		return o.toString();
	}

}
