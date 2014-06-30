/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public interface PlayExtra extends Serializable {
	public long lastStamp();

	public String getSessionId();

	public PlayType getPlayType();

	public static enum PlayType {
		BGN, END, ALIVE, EMPTY;

		public boolean isBgn() {
			return this == BGN;
		}

		public boolean isEnd() {
			return this == END;
		}

		public boolean isAlive() {
			return this == ALIVE;
		}

		public boolean isEmpty() {
			return this == EMPTY;
		}
	}
}
