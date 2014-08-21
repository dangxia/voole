/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.common.enums;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public enum ProductType {
	FREE, // 免费
	ORDER, // 包月
	BIG_PAGCKAGE, // 大包
	BOUTIQUE_PAGCKAGE, // 精品包
	OTHER;// 其他
	public static ProductType getProductType(int ptype, int fee) {
		if (fee == 0) {
			return ProductType.FREE;
		} else if (fee < 0) {
			return ProductType.OTHER;
		}
		if (ptype == 0) {
			return ProductType.FREE;
		} else if (ptype <= 100) {
			return ProductType.ORDER;
		} else if (ptype >= 200) {
			return ProductType.BIG_PAGCKAGE;
		} else if (ptype == 300) {
			return ProductType.BOUTIQUE_PAGCKAGE;
		}
		return ProductType.OTHER;
	}

	/**
	 * 是否收费
	 * 
	 * @param productType
	 * @return boolean
	 */
	public static boolean isFee(ProductType productType) {
		return productType != ProductType.FREE
				&& productType != ProductType.OTHER;
	}
}
