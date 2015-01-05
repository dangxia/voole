package com.voole.hobbit2.storm.order;

import java.util.Map;
import java.util.Map.Entry;

import com.voole.monitor2.playurl.PlayurlAnalyzer;

public class TestUrl {
	public static void main(String[] args) {
		String url = "?uid=&epgid=600033&fid=a6df871c06c446c014a7351080ad875a&mid=4759820&sid=17&pid=&secid=B2BSTBOX_cartoon_youngblood";
		Map<String, String> map = PlayurlAnalyzer.analyze(url);
		for (Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}
}
