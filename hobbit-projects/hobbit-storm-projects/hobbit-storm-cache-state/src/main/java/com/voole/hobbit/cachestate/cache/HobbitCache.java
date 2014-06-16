/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.cache;

import org.slf4j.Logger;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface HobbitCache {
	public void refreshDelay();

	public void refreshImmediately();

	public String getName();

	public abstract static class AbstractHobbitCache implements HobbitCache {
		private final String name;
		private boolean isFetching;
		private boolean delayIsReady;

		public AbstractHobbitCache(String name) {
			this.name = name;
			this.isFetching = false;
			this.delayIsReady = false;
		}

		protected abstract void _swop();

		protected abstract void _fetch();

		protected abstract Logger getLogger();

		protected void fetch() {
			isFetching = true;
			getLogger().info(getName() + " fetch started");
			_fetch();
			getLogger().info(getName() + " fetch ended");
			delayIsReady = true;
			isFetching = false;
		}

		protected void swop() {
			if (delayIsReady) {
				getLogger().info(getName() + " swop started");
				_swop();
				getLogger().info(getName() + " swop ended");
				delayIsReady = false;
			}
		}

		@Override
		public void refreshDelay() {
			if (!isFetching) {
				isFetching = true;
				getLogger().info(getName() + " run refreshDelay");
				new FetchThread().start();
			}
		}

		@Override
		public void refreshImmediately() {
			if (!isFetching) {
				getLogger().info(getName() + " run refreshImmediately");
				fetch();
				swop();
			}
		}

		@Override
		public String getName() {
			return this.name;
		}

		class FetchThread extends Thread {
			@Override
			public void run() {
				fetch();
			}
		}

	}

}
