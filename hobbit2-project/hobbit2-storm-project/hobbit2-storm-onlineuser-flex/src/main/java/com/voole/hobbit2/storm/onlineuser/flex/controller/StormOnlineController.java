package com.voole.hobbit2.storm.onlineuser.flex.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

public interface StormOnlineController {
	void getGridData(String spid, HttpServletResponse response)
			throws IOException;

	void getTotalFlexChartData(String spid, String stampStr,
			HttpServletResponse response) throws IOException;

	void getParentFlexChartData(String spid, String stampStr,
			HttpServletResponse response) throws IOException;

	void getChildFlexChartData(Long oemid, String stampStr,
			HttpServletResponse response) throws IOException;
}
