package com.voole.hobbit2.storm.onlineuser.flex.controller;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.OemTrait;
import com.voole.hobbit2.storm.onlineuser.flex.model.grid.SpTrait;
import com.voole.hobbit2.storm.onlineuser.flex.service.OnlineUserStateFlexService;

@Controller
public class StormOnlineControllerImpl implements StormOnlineController {
	@Autowired
	private OnlineUserStateFlexService service;
	private final Gson gson;

	public StormOnlineControllerImpl() {
		GsonBuilder gb = new GsonBuilder();
		gb.registerTypeAdapter(Date.class, new JsonSerializer<Date>() {
			@Override
			public JsonElement serialize(Date src, Type typeOfSrc,
					JsonSerializationContext context) {
				if (src != null) {
					return context.serialize(src.getTime());
				}
				return null;
			}
		});
		gb.setPrettyPrinting();
		gson = gb.create();
	}

	@Override
	@RequestMapping("/order/online_storm/getGridData.do")
	public void getGridData(
			@RequestParam(value = "spid", required = false) String spid,
			HttpServletResponse response) throws IOException {
		response.getWriter().write(gson.toJson(service.getGridData(spid)));
	}

	@Override
	@RequestMapping("/order/online_storm/getTotalFlexChartData.do")
	public void getTotalFlexChartData(
			@RequestParam(value = "spid", required = false) String spid,
			@RequestParam(value = "stamp", required = false) String stampStr,
			HttpServletResponse response) throws IOException {
		Date stamp = getStamp(stampStr);
		response.getWriter().write(
				gson.toJson(service.getTotalFlexChartData(spid, stamp)));

	}

	protected Date getStamp(String stampStr) {
		Date stamp = null;
		if (stampStr != null && stampStr.length() > 0) {
			stamp = new Date(Long.parseLong(stampStr));
		}
		return stamp;
	}

	@Override
	@RequestMapping("/order/online_storm/getParentFlexChartData.do")
	public void getParentFlexChartData(
			@RequestParam(value = "spid") String spid,
			@RequestParam(value = "stamp", required = false) String stampStr,
			HttpServletResponse response) throws IOException {
		Date stamp = getStamp(stampStr);
		response.getWriter().write(
				gson.toJson(service.getParentFlexChartData(new SpTrait(spid),
						stamp)));

	}

	@Override
	@RequestMapping("/order/online_storm/getChildFlexChartData.do")
	public void getChildFlexChartData(
			@RequestParam(value = "oemid") Long oemid,
			@RequestParam(value = "stamp", required = false) String stampStr,
			HttpServletResponse response) throws IOException {
		Date stamp = getStamp(stampStr);
		OemTrait oemTrait = new OemTrait();
		oemTrait.setOemid(oemid);
		response.getWriter().write(
				gson.toJson(service.getChildFlexChartData(oemTrait, stamp)));
	}

}
