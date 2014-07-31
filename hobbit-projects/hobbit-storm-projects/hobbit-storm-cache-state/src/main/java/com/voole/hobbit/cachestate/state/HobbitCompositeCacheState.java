/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cachestate.state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.cache.HobbitCache;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class HobbitCompositeCacheState implements HobbitState {
	private final List<HobbitCache> caches;

	private static final Logger logger = LoggerFactory
			.getLogger(HobbitCompositeCacheState.class);

	public HobbitCompositeCacheState(HobbitCache... caches) {
		if (caches.length < 1) {
			throw new IllegalArgumentException("caches length = 0");
		}
		this.caches = new ArrayList<HobbitCache>(Arrays.asList(caches));
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	@Override
	public void refreshDelay(List<String> cmds) {
		for (HobbitCache cache : caches) {
			if (isShouldRefresh(cmds, cache)) {
				cache.refreshDelay();
			}
		}
	}

	@Override
	public void refreshImmediately(List<String> cmds) {
		for (HobbitCache cache : caches) {
			if (isShouldRefresh(cmds, cache)) {
				cache.refreshImmediately();
			}
		}
	}

	protected boolean isShouldRefresh(List<String> cmds, HobbitCache cache) {
		if (cmds.size() == 0) {
			return false;
		}
		String stateName = cache.getName();
		for (String cmd : cmds) {
			if (cmd != null && (stateName.equals(cmd) || "all".equals(cmd))) {
				getLogger().info(
						stateName + " receive cmd:" + cmd
								+ " which permit for refresh");
				return true;
			}
		}
		return false;
	}

	public List<HobbitCache> getCaches() {
		return caches;
	}

	protected Logger getLogger() {
		return logger;
	}
}
