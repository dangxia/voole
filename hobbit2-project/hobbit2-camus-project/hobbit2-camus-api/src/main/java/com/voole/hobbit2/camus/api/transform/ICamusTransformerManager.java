package com.voole.hobbit2.camus.api.transform;

import com.voole.hobbit2.camus.api.ICamusKey;

public interface ICamusTransformerManager {
	public <Source, Target> ICamusTransformer<Source, Target> findTransformer(
			ICamusKey key);
}
