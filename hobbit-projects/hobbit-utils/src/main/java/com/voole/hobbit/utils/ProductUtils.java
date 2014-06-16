/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.utils;

/**
 * @author XuehuiHe
 * @date 2014年6月16日
 */
public class ProductUtils {
	public static final Long VOOLE_SPID = 10002001l;

	public enum ProductType {
		FREE, ORDER, BIG_PAGCKAGE, BOUTIQUE_PAGCKAGE, OTHER;
	}

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

	public static boolean isFee(ProductType productType) {
		return productType != ProductType.FREE
				&& productType != ProductType.OTHER;
	}
}
