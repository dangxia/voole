/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class MemoryTimeoutMapState<T extends TimeoutAbled> implements
		TimeoutState<T> {
	private Long _currTx;
	private final TreeMap<Long, Set<List<Object>>> timeoutTree;
	private final Map<List<Object>, OpaqueValue<T>> db;

	public MemoryTimeoutMapState() {
		timeoutTree = new TreeMap<Long, Set<List<Object>>>();
		db = new HashMap<List<Object>, OpaqueValue<T>>();
	}

	@Override
	public void beginCommit(Long txid) {
		_currTx = txid;
	}

	@Override
	public void commit(Long txid) {
		_currTx = null;
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> ret = new ArrayList<T>();
		for (List<Object> key : keys) {
			ret.add(db.get(key).get(_currTx));
		}
		return ret;
	}

	protected List<OpaqueValue<T>> multiGetOpaqueValues(List<List<Object>> keys) {
		List<OpaqueValue<T>> ret = new ArrayList<OpaqueValue<T>>();
		for (List<Object> key : keys) {
			ret.add(db.get(key));
		}
		return ret;
	}

	@Override
	public List<T> multiPut(List<List<Object>> keys, List<T> vals) {
		List<OpaqueValue<T>> curr = multiGetOpaqueValues(keys);
		List<OpaqueValue<T>> newVals = new ArrayList<OpaqueValue<T>>(
				curr.size());
		List<T> ret = new ArrayList<T>();
		// for (int i = 0; i < curr.size(); i++) {
		// OpaqueValue<T> val = curr.get(i);
		// ReplaceUpdater<T> updater = updaters.get(i);
		// T prev;
		// if (val == null) {
		// prev = null;
		// } else {
		// prev = val.get(_currTx);
		// }
		// T newVal = updater.update(prev);
		// ret.add(newVal);
		// OpaqueValue<T> newOpaqueVal;
		// if (val == null) {
		// newOpaqueVal = new OpaqueValue<T>(_currTx, newVal);
		// } else {
		// newOpaqueVal = val.update(_currTx, newVal);
		// }
		// newVals.add(newOpaqueVal);
		// }
		// _backing.multiPut(keys, newVals);
		return ret;
		// for (int i = 0; i < keys.size(); i++) {
		// List<Object> key = keys.get(i);
		// T val = vals.get(i);
		// put(key, val);
		// }
	}

	protected void put(List<Object> key, T newval) {
//		T oldval = db.get(key);
//		if (oldval != null) {
//			Long oldStamp = oldval.getStamp();
//			Set<List<Object>> keys = timeoutTree.get(oldStamp);
//			keys.remove(key);
//			if (keys.isEmpty()) {
//				timeoutTree.remove(oldStamp);
//			}
//		}
//
//		Long newStamp = newval.getStamp();
//		Set<List<Object>> keys = null;
//		if (!timeoutTree.containsKey(newStamp)) {
//			keys = new HashSet<List<Object>>();
//			timeoutTree.put(newStamp, keys);
//		} else {
//			keys = timeoutTree.get(newStamp);
//		}
//		keys.add(key);
//		db.put(key, newval);
	}

	@Override
	public long getMaxTimeoutedStamp() {
		return 0;
	}

	@Override
	public List<T> timeout() {
		return null;
	}

	class MemoryMapStateBacking implements IBackingMap<T>,
			IBackingMapTimeoutState<T> {
		private final TreeMap<Long, Set<List<Object>>> timeoutTree;
		private final Map<List<Object>, T> db;

		public MemoryMapStateBacking(long timeout) {
			timeoutTree = new TreeMap<Long, Set<List<Object>>>();
			db = new HashMap<List<Object>, T>();
		}

		@Override
		public List<T> multiGet(List<List<Object>> keys) {
			List<T> ret = new ArrayList<T>();
			for (List<Object> key : keys) {
				ret.add(db.get(key));
			}
			return ret;
		}

		@Override
		public void multiPut(List<List<Object>> keys, List<T> vals) {
			for (int i = 0; i < keys.size(); i++) {
				List<Object> key = keys.get(i);
				T val = vals.get(i);
				put(key, val);
			}
		}

		protected void put(List<Object> key, T newval) {
			T oldval = db.get(key);
			if (oldval != null) {
				Long oldStamp = oldval.getStamp();
				Set<List<Object>> keys = timeoutTree.get(oldStamp);
				keys.remove(key);
				if (keys.isEmpty()) {
					timeoutTree.remove(oldStamp);
				}
			}

			Long newStamp = newval.getStamp();
			Set<List<Object>> keys = null;
			if (!timeoutTree.containsKey(newStamp)) {
				keys = new HashSet<List<Object>>();
				timeoutTree.put(newStamp, keys);
			} else {
				keys = timeoutTree.get(newStamp);
			}
			keys.add(key);
			db.put(key, newval);
		}

		@Override
		public List<T> timeout() {
			long maxTimeoutedStamp = getMaxTimeoutedStamp();
			List<Long> timeoutTreeKeys = new LinkedList<Long>();
			List<List<Object>> timeoutDbKeys = new LinkedList<List<Object>>();
			for (Entry<Long, Set<List<Object>>> entry : timeoutTree.headMap(
					maxTimeoutedStamp, true).entrySet()) {
				timeoutTreeKeys.add(entry.getKey());
				timeoutDbKeys.addAll(entry.getValue());
			}
			for (Long treeKey : timeoutTreeKeys) {
				timeoutTree.remove(treeKey);
			}
			List<T> ret = new ArrayList<T>();
			for (List<Object> dbKey : timeoutDbKeys) {
				ret.add(db.remove(dbKey));
			}

			return ret;
		}
	}

}
