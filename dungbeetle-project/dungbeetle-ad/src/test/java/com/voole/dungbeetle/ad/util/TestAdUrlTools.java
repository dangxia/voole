package com.voole.dungbeetle.ad.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.voole.dungbeetle.ad.model.PlayUrlAdInfos;

public class TestAdUrlTools {
	public static String mp4Playurl = "vosp://cdn.voole.com:3528/play?fid=dd784b96b4498b306e9e1e1b1a8b08c7&uid=104066457&stamp=1413806498&keyid=67141632&s=14&auth=a7cb893a3909cdbe230538b4a49c754c&ext=admt:2@lg:139ed984f559c596:pln:111:pln:222:pln:333@2973058958057b1e3b31180e4936fccc:0:35,code:,oid:156,eid:100115,pid:101002,po:100027&stime=0&etime=0&is3d=0&bke=117.79.132.26&proto=6&tvid=39E680F-K028155-G131115-91862-001A9A00001";
	public static String m3u8Playurl = "http://cdn.voole.com:3580/uid$5013640/stamp$1413807482/keyid$67141632/ auth$5381aaab70ceb9dc1c022c15fe22d608/6be9c6883ca68a5650adb036421325d2.m3u8?ext= btime:0,etime:0;oid:699,eid:100897,code:52045976,pid:101005,po:100033;admt:4,lg: 139eda5c607fb51a:pln:317,145968c1a793e6e6ad7395185f383bd5:0:35:5929708&bke=&type =get_m3u8&host=cdn.voole.com:3580&port=3528&is3d=0&proto=5";

	public static String mp4AdPart = "admt:2@lg:139ed984f559c596:pln:111:pln:222:pln:333@2973058958057b1e3b31180e4936fccc:0:35"
			.trim().toLowerCase();

	public static String m3u8AdPart = "admt:4,lg: 139eda5c607fb51a:pln:317,145968c1a793e6e6ad7395185f383bd5:0:35:5929708"
			.trim().toLowerCase();

	@Test
	public void testMp4AdPart() {
		String adPart = AdUrlTools.getUrlAdPart(mp4Playurl);
		Assert.assertEquals(mp4AdPart, adPart);
	}

	@Test
	public void testM3u8AdPart() {
		String adPart = AdUrlTools.getUrlAdPart(m3u8Playurl);
		Assert.assertEquals(m3u8AdPart, adPart);
	}

	@Test
	public void testUrls() throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				TestAdUrlTools.class.getClassLoader().getResourceAsStream(
						"urls.txt")));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				Optional<PlayUrlAdInfos> adInfos = AdUrlTools
						.parsePlayUrl(line);
				Assert.assertTrue(line, adInfos.isPresent());
				System.out.println(adInfos.get());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reader.close();
		}

	}
}
