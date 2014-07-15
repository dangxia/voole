package com.voole.hobbit.camus.etl.kafka.common;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
	public static DateTimeZone CTT = DateTimeZone.forID("Asia/Shanghai");
	public static DateTimeFormatter MINUTE_FORMATTER = getDateTimeFormatter("YYYY-MM-dd-HH-mm");

	public static DateTimeFormatter getDateTimeFormatter(String str) {
		return getDateTimeFormatter(str, CTT);
	}

	public static DateTimeFormatter getDateTimeFormatter(String str,
			DateTimeZone timeZone) {
		return DateTimeFormat.forPattern(str).withZone(timeZone);
	}

	public static long getPartition(long timeGranularityMs, long timestamp) {
		return (timestamp / timeGranularityMs) * timeGranularityMs;
	}

	public static DateTime getMidnight() {
		DateTime time = new DateTime(CTT);
		return new DateTime(time.getYear(), time.getMonthOfYear(),
				time.getDayOfMonth(), 0, 0, 0, 0, CTT);
	}

}