package com.voole.hobbit2.camus.api.partitioner;

import com.voole.hobbit2.camus.api.ICamusKey;

public interface ICamusPartitionerManager {
	public <Key extends ICamusKey, Value> ICamusPartitioner<Key, Value> findPartitioner(
			Key key, Value value);
}
