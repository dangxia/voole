/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.voole.hobbit2.cache.exception.CacheQueryException;
import com.voole.hobbit2.cache.exception.CacheRefreshException;

/**
 * @author XuehuiHe
 * @date 2014年6月13日
 */
public interface HobbitCache {
	public void refresh() throws CacheRefreshException;

	public abstract static class AbstractHobbitCache implements HobbitCache {
		private final Logger logger = LoggerFactory
				.getLogger(AbstractHobbitCache.class);
		private final ReentrantReadWriteLock readWriteLock;
		private final Lock read;
		private final Lock write;

		private volatile boolean isRefreshFailed;

		private final AtomicBoolean isFetching;
		private volatile CountDownLatch fetchLatch;

		public AbstractHobbitCache() {
			isRefreshFailed = false;
			// lock
			readWriteLock = new ReentrantReadWriteLock();
			read = readWriteLock.readLock();
			write = readWriteLock.writeLock();

			isFetching = new AtomicBoolean(false);
		}

		protected void checkRefreshBeforeQuery() throws CacheRefreshException {
			if (isRefreshFailed()) {
				throw new CacheRefreshException(getClass()
						+ "want to query but refresh has failed!");
			}
		}

		public <F, T> T query(Function<F, T> function, F f)
				throws CacheRefreshException, CacheQueryException {
			read.lock();
			checkRefreshBeforeQuery();
			try {
				return function.apply(f);
			} catch (Exception e) {
				throw new CacheQueryException(getClass() + " query failed", e);
			} finally {
				read.unlock();
			}
		}

		protected synchronized boolean isShouldFetchCurrThread() {
			if (!isFetching.compareAndSet(false, true)) {
				return false;
			} else {
				fetchLatch = new CountDownLatch(1);
				return true;
			}
		}

		protected void releaseFetchLock() {
			if (fetchLatch != null) {
				fetchLatch.countDown();
				fetchLatch = null;
			}
			isFetching.set(false);
		}

		@Override
		public void refresh() throws CacheRefreshException {
			boolean isShouldFetchCurrThread = isShouldFetchCurrThread();

			if (!isShouldFetchCurrThread) {
				getLogger().info(
						getClass() + " other thread fetching ,waiting ...");
				try {
					fetchLatch.await();
				} catch (InterruptedException e1) {
					return;
				}
				getLogger().info(getClass() + " other thread fetch finished");
				return;
			}
			getLogger().info(getClass() + " refresh starting");
			long temp = System.currentTimeMillis();
			try {
				fetch();
			} catch (Exception e) {
				isRefreshFailed = true;
				releaseFetchLock();
				throw new CacheRefreshException(getClass() + " refresh error",
						e);
			}
			write.lock();
			try {
				swop();
			} catch (Exception e) {
				isRefreshFailed = true;
				throw new CacheRefreshException(getClass() + " swop error", e);
			} finally {
				write.unlock();
				releaseFetchLock();
			}
			isRefreshFailed = false;
			long used = (System.currentTimeMillis() - temp) / 1000;
			getLogger().info(
					getClass() + " refresh end,used:" + used + " seconds");
		}

		protected abstract void swop();

		protected abstract void fetch();

		public boolean isRefreshFailed() {
			return isRefreshFailed;
		}

		public void setRefreshFailed(boolean isRefreshFailed) {
			this.isRefreshFailed = isRefreshFailed;
		}

		public ReentrantReadWriteLock getReadWriteLock() {
			return readWriteLock;
		}

		public Lock getRead() {
			return read;
		}

		public Lock getWrite() {
			return write;
		}

		public Logger getLogger() {
			return logger;
		}

	}

	public static class TestAbstractHobbitCache extends AbstractHobbitCache {

		@Override
		protected void swop() {
			// TODO Auto-generated method stub

		}

		@Override
		protected void fetch() {
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public static void main(String[] args) throws InterruptedException,
				CacheRefreshException {
			final TestAbstractHobbitCache cache = new TestAbstractHobbitCache();
			new Thread() {
				@Override
				public void run() {
					try {
						cache.refresh();
					} catch (CacheRefreshException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
			TimeUnit.SECONDS.sleep(1);
			cache.refresh();
			System.out.println("-----end");
			new Thread() {
				@Override
				public void run() {
					try {
						TimeUnit.SECONDS.sleep(1);
						cache.refresh();
					} catch (CacheRefreshException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
			new Thread() {
				@Override
				public void run() {
					try {
						TimeUnit.SECONDS.sleep(1);
						cache.refresh();
					} catch (CacheRefreshException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
			// TimeUnit.SECONDS.sleep(1);
			cache.refresh();
			System.out.println("-----end");
		}
	}

}
