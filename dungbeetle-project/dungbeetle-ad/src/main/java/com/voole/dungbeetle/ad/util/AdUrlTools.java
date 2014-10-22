package com.voole.dungbeetle.ad.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.voole.dungbeetle.ad.exception.AdUrlTransformException;
import com.voole.dungbeetle.ad.model.PlayUrlAdInfos;
import com.voole.dungbeetle.ad.model.PlayUrlAdInfos.PlayUrlAdItem;

public class AdUrlTools {

	private static final Logger log = LoggerFactory.getLogger(AdUrlTools.class);

	public static final String EXT = "ext";

	public static Optional<PlayUrlAdInfos> parsePlayUrl(String playurl)
			throws AdUrlTransformException {
		if (!StringUtils.isNotBlank(playurl)) {
			return Optional.absent();
		}
		String urlAdPart = getUrlAdPart(playurl);
		if (!StringUtils.isNotBlank(urlAdPart)) {
			return Optional.absent();
		}
		String[] items = urlAdPart.split("[,@]");
		if (items.length > 2 && items[0].indexOf("admt") != -1
				&& items[1].indexOf("lg") != -1) {
			PlayUrlAdInfos adInfos = new PlayUrlAdInfos();
			adInfos.setMergetype(getMergetype(items[0]));
			fillLogIdAndPlnListAndMid(adInfos, items[1]);
			for (int i = 2; i < items.length; i++) {
				adInfos.addItem(createAdItem(items[i]));
			}
			return Optional.of(adInfos);
		} else {
			throw new AdUrlTransformException("illegal admt url:" + urlAdPart);
		}

	}

	public static void fillLogIdAndPlnListAndMid(PlayUrlAdInfos adInfos, String logStr)
			throws AdUrlTransformException {
		String[] strs = logStr.split(":");
		boolean isSetLg = false;
		List<String> plnidList = new ArrayList<String>();
		if (strs.length % 2 != 0) {
			throw new AdUrlTransformException("lg url split by :, %2!=0");
		}
		for (int i = 0; i < strs.length; i += 2) {
			String key = strs[i].trim();
			String value = strs[i + 1].trim();
			if ("lg".equals(key) && !isSetLg) {
				if (value.length() == 0) {
					adInfos.setLogid("-1");
				} else {
					adInfos.setLogid(Long.valueOf(value, 16).toString());
				}
				isSetLg = true;
			} else if ("pln".equals(key)) {
				plnidList.add(value);
			} else if("mid".equals(key)){
				adInfos.setMid(value);
			}else{
				log.warn("key:" + key + " is not lg nor pln");
			}
		}
		adInfos.setPlnidList(plnidList);
	}
	
	public static void main(String[] args) throws AdUrlTransformException {
		
	}
	
	public static PlayUrlAdItem createAdItem(String itemUrl)
			throws AdUrlTransformException {
		String[] strs = itemUrl.split(":");
		if (strs.length < 3) {
			throw new AdUrlTransformException("adItemUrl split by :,length < 3");
		}
		PlayUrlAdItem item = new PlayUrlAdItem();
		item.setFid(strs[0].trim());
		item.setStartTime(Integer.parseInt(strs[1].trim()));
		item.setPlayTime(Integer.parseInt(strs[2].trim()));
		if (strs.length > 3) {
			item.setAdSize(strs[3].trim());
		}
		return item;
	}

	public static int getMergetype(String admt) throws AdUrlTransformException {
		int index = admt.indexOf(":");
		if (index == -1) {
			throw new AdUrlTransformException("admt:" + admt + " not found!");
		}
		if (admt.length() == index + 1) {
			throw new AdUrlTransformException("admt:" + admt + " not found!");
		}
		return Integer.parseInt(admt.substring(index + 1).trim());
	}

	public static String getUrlAdPart(String playurl) {
		String playurlExtStart = "";
		if (playurl.indexOf(EXT) > -1) {
			String exturl = playurl.substring(playurl.indexOf(EXT) + 4);
			if (playurl.indexOf(".m3u8") > -1) { // m3u8格式处理
				String[] m3u8Arr = exturl.split(";");
				for (String m3u8 : m3u8Arr) {
					if (m3u8.indexOf("admt") > -1) {
						playurlExtStart = m3u8.substring(m3u8.indexOf("admt"),
								m3u8.indexOf("&"));
						break;
					}
				}
			} else {// MP4处理格式
				String[] mp4Arr = exturl.split(",");
				for (String mp4 : mp4Arr) {
					if (mp4.indexOf("admt") > -1) {
						playurlExtStart = mp4;
						break;
					}
				}
			}
		}
		return playurlExtStart.trim().toLowerCase();
	}

}
