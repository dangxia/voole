package com.voole;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class NodeServer {
	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				NodeServer.class.getClassLoader().getResourceAsStream(
						"nodeserver")));

		List<String> names = new ArrayList<String>();
		List<String> lens = new ArrayList<String>();
		List<String> types = new ArrayList<String>();
		List<String> descs = new ArrayList<String>();

		String line = null;
		int i = 0;
		while ((line = reader.readLine()) != null) {
			int mod = i % 4;
			switch (mod) {
			case 0:
				names.add(line);
				break;
			case 1:
				lens.add(line);
				break;
			case 2:
				types.add(line);
				break;
			case 3:
				descs.add(line);
				break;
			default:
				break;
			}
			i++;
		}

		reader.close();

		int nameLen = getLen(names);
		int lenLen = getLen(lens);
		int typeLen = getLen(types);
		int descLen = getLen(descs);

		int len = names.size();
		for (int j = 0; j < len; j++) {
			System.out.println("|" + appendSpace(names.get(j), nameLen) + "|"
					+ appendSpace(lens.get(j), lenLen) + "|"
					+ appendSpace(types.get(j), typeLen) + "|"
					+ appendSpace(descs.get(j), descLen) + "|");
		}

	}

	public static String appendSpace(String source, int len) {
		int strLen = source.trim().length();
		int diff = len - strLen;
		StringBuffer sb = new StringBuffer(source.trim());
		for (int i = 0; i < diff; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}

	public static int getLen(List<String> list) {
		int max = 0;
		for (String item : list) {
			int len = item.trim().length();
			if (len > max) {
				max = len;
			}
		}
		return (int) Math.ceil(max * 1.2);
	}
}
