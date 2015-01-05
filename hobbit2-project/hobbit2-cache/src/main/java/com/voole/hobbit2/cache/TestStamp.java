package com.voole.hobbit2.cache;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestStamp {
	public static void main(String[] args) throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(df.parse("2014-12-30 19:23:19").getTime() / 1000);
		System.out.println(df.parse("2014-12-30 19:00:19").getTime() / 1000);
	}
}
