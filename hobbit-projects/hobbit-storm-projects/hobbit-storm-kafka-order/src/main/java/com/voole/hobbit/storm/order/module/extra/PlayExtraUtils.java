/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.module.extra;

import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayBgnReqV3;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV2;
import com.voole.hobbit.proto.TerminalPB.OrderPlayEndReqV3;

/**
 * @author XuehuiHe
 * @date 2014年6月24日
 */
public class PlayExtraUtils {
	public static PlayExtra getExtra(Object proto) {
		if (proto instanceof OrderPlayAliveReqV2
				|| proto instanceof OrderPlayAliveReqV3) {
			return PlayAliveExtra.getExtra(proto);
		} else if (proto instanceof OrderPlayBgnReqV2
				|| proto instanceof OrderPlayBgnReqV3) {
			return PlayBgnExtra.getExtra(proto);
		} else if (proto instanceof OrderPlayEndReqV2
				|| proto instanceof OrderPlayEndReqV3) {
			return PlayEndExtra.getExtra(proto);
		}
		return null;
	}
}
