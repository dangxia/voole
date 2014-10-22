package com.voole.dungbeetle.ad.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;



/**
 * 时间工具类
 * 
 * @author Administrator
 *
 */
public class DateUtil {

	private static Logger logger = Logger.getLogger(DateUtil.class);

	public static String getCurrentTime() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(new Date());
	}

	public static String getCurrentDate() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(new Date());
	}

	public static String getCurrentString(String format) {
		return getDateString(new Date(), format);
	}

	public static String getDateString(Date date, String format) {
		SimpleDateFormat formater = new SimpleDateFormat(format);
		return formater.format(date);
	}

	public static String getCurrentTimeWithMS() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		return format.format(new Date());
	}

	public static long getMinuteBetween(Date date1, Date date2) {
		long result = 0L;
		try {
			result = (date2.getTime() - date1.getTime()) / 60000;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public static Date date = null;

	public static DateFormat dateFormat = null;

	public static Calendar calendar = null;

	public static Date parseDate(String dateStr, String format) {
		try {
			dateFormat = new SimpleDateFormat(format);
			// String dt = dateStr.replaceAll("-", "/");
			if ((!dateStr.equals("")) && (dateStr.length() < format.length())) {
				dateStr += format.substring(dateStr.length()).replaceAll("[YyMmDdHhSs]", "0");
			}
			date = (Date) dateFormat.parse(dateStr);
		} catch (Exception e) {
		}
		return date;
	}

	public static Date parseDate(String dateStr) {
		return parseDate(dateStr, "yyyy/MM/dd");
	}

	public static String format(Date date, String format) {
		String result = "";
		try {
			if (date != null) {
				dateFormat = new SimpleDateFormat(format);
				result = dateFormat.format(date);
			}
		} catch (Exception e) {
		}
		return result;
	}

	public static String format(Date date) {
		return format(date, "yyyy-MM-dd HH:mm:ss");
	}

	public static int getYear(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	public static int getMonth(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MONTH) + 1;
	}

	public static int getDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	public static int getHour(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	public static int getMinute(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MINUTE);
	}

	public static int getSecond(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.SECOND);
	}

	public static long getMillis(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}

	public static String getDate(Date date) {
		return format(date, "yyyy/MM/dd");
	}

	public static String getTime(Date date) {
		return format(date, "HH:mm:ss");
	}

	public static String getDateTime(Date date) {
		return format(date, "yyyy/MM/dd HH:mm:ss");
	}

	public static Date addDate(Date date, int day) {
		Calendar calendar = Calendar.getInstance();
		long millis = getMillis(date) + ((long) day) * 24 * 3600 * 1000;
		calendar.setTimeInMillis(millis);
		return calendar.getTime();
	}

	public static int diffDate(Date date, Date date1) {
		return (int) ((getMillis(date) - getMillis(date1)) / (24 * 3600 * 1000));
	}

	public static String getMonthBegin(String strdate) {
		date = parseDate(strdate);
		return format(date, "yyyy-MM") + "-01";
	}

	public static String getMonthEnd(String strdate) {
		date = parseDate(getMonthBegin(strdate));
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, 1);
		calendar.add(Calendar.DAY_OF_YEAR, -1);
		return formatDate(calendar.getTime());
	}

	public static String formatDate(Date date) {
		return formatDateByFormat(date, "yyyy-MM-dd");
	}

	public static String formatDateByFormat(Date date, String format) {
		String result = "";
		if (date != null) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(format);
				result = sdf.format(date);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * 给定日期获取中文星期
	 * 
	 * @param date
	 * @return
	 */
	public static String getWeekZh(Date date) {
		String[] weekDays = { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);

		int week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
		if (week < 0) {
			week = 0;
		}

		return weekDays[week];
	}

	/**
	 * 将HH:mm:ss 格式时间转化为秒数
	 * 
	 * @param time
	 * @return
	 */
	public static Integer timeFormat(String time) {
		if (StringUtils.isBlank(time)) {
			return null;
		}

		String[] strs = time.split(":");
		return Integer.parseInt(strs[0]) * 3600 + Integer.parseInt(strs[1]) * 60
				+ Integer.parseInt(strs[2]);
	}

	/**
	 * 将秒数转化成HH:mm:ss时间格式
	 * 
	 * @param time
	 * @return
	 */
	public static String timeParse(Integer time) {
		int h = time / 3600;
		int m = (time - h * 3600) / 60;
		int s = time % 60;
		return h + ":" + m + ":" + s;
	}

	/**
	 * 日期计算
	 * 
	 * @param date
	 *            按某个日期计算
	 * @param calendarType
	 *            如Calendar.DAY_OF_WEEK等
	 * @param digit
	 *            具体计算数字
	 * @return
	 */
	public static Date dateDiff(Date date, int calendarType, int digit) {
		if (date != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(calendarType, digit);
			return calendar.getTime();
		}
		return null;
	}
	
	/**
	 * 含有英文格式的字符串
	 *   英文字符串格式必须是：月 日 ,年 小时：分钟：秒 am/pm 如："May 15, 2014 5:13:00 PM";
	 * @param dateStr
	 * @return
	 * @throws ParseException 
	 */
	public static Date parseStringToDate(String dateStr) throws ParseException{
		
	       SimpleDateFormat sdf1 = new SimpleDateFormat("MMM d,yyyy hh:mm:ss a", Locale.US);
	      
			return sdf1.parse(dateStr);
		
	     
	}
	/**
	 * 含有英文格式的字符串
	 *   英文字符串格式必须是：月 日 ,年 小时：分钟：秒 am/pm 如："May 15, 2014 5:13:00 PM";
	 * @param dateStr
	 * @return
	 */
	public static String parseFormat(String dateStr) {
		String rtnDate = "";
		if (StringUtils.isNotBlank(dateStr)) {
			try {
				SimpleDateFormat sdf1 = new SimpleDateFormat("MMM d,yyyy hh:mm:ss a", Locale.US);
				rtnDate = format(sdf1.parse(dateStr));
			} catch (ParseException e) {
				logger.error("字符串转化时间异常，异常信息为"+ e.getMessage());
				e.printStackTrace(); 
			}
		}
		return rtnDate;
	}

	
	/**
	 * 获取今天是周几（数字：1、2、3...）
	 * @return
	 */
	public static int getDayOfWeek(){
		Calendar calendar = Calendar.getInstance();
		return calendar.get(calendar.DAY_OF_WEEK);
	}
	
	/**
	 * 获取date是周几（数字：1、2、3...）
	 * @return
	 */
	public static int getDayOfWeek(Date date){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(calendar.DAY_OF_WEEK);
	}
}
