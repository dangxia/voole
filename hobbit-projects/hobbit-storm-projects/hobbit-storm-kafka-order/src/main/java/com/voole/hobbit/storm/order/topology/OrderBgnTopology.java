/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.storm.order.topology;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.tuple.Fields;

import com.voole.hobbit.cachestate.state.HobbitState.HobbitAreaInfoStateFactory;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitOemInfoStateFactory;
import com.voole.hobbit.cachestate.state.HobbitState.HobbitResourceInfoStateFactory;
import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.storm.order.function.AssemblySession;
import com.voole.hobbit.storm.order.function.aggregator.OnlineUserModifierCombiner;
import com.voole.hobbit.storm.order.function.extra.PlayBgnExtraFunction;
import com.voole.hobbit.storm.order.state.HidTickStateImpl.HidTickStateFactory;
import com.voole.hobbit.storm.order.state.OnlineUserStateImpl.OnlineUserStateFactory;
import com.voole.hobbit.storm.order.state.SessionStateImpl.SessionStateFactory;
import com.voole.hobbit.storm.order.state.query.AreaInfoQueryFunction;
import com.voole.hobbit.storm.order.state.query.ResourceQueryFunction;
import com.voole.hobbit.storm.order.state.query.SpidQueryFunction;
import com.voole.hobbit.storm.order.state.updater.HidTickStateUpdater;
import com.voole.hobbit.storm.order.state.updater.OnlineUserStateQueryUpdater;
import com.voole.hobbit.storm.order.state.updater.SessionStateUpdater;

/**
 * @author XuehuiHe
 * @date 2014年6月6日
 */
public class OrderBgnTopology extends OrderTopology {

	// private static Logger log =
	// LoggerFactory.getLogger(OrderBgnTopology.class);
	private TridentState oemInfoState;
	private TridentState areaInfoState;
	private TridentState resourceInfoState;
	private TridentState onlineUserState;
	private TridentState sessionState;
	private TridentState hidTickState;

	public OrderBgnTopology() {
		super("order-bgn-kafka-stream", TopicProtoClassUtils.ORDER_BGN_V2,
				TopicProtoClassUtils.ORDER_BGN_V3);
	}

	@Override
	public Stream build(TridentTopology topology) {
		Stream stream = super.build(topology);
		oemInfoState = topology.newStaticState(new HobbitOemInfoStateFactory());
		areaInfoState = topology
				.newStaticState(new HobbitAreaInfoStateFactory());
		resourceInfoState = topology
				.newStaticState(new HobbitResourceInfoStateFactory());
		onlineUserState = topology.newStaticState(new OnlineUserStateFactory());
		Stream bgnStream = stream.each(
				PlayBgnExtraFunction.INPUT_FIELDS,
				new PlayBgnExtraFunction(),
				PlayBgnExtraFunction.OUTPUT_FIELDS).project(
				PlayBgnExtraFunction.OUTPUT_FIELDS);
		sessionState = bgnStream
				.stateQuery(oemInfoState, SpidQueryFunction.INPUT_FIELDS,
						new SpidQueryFunction(),
						SpidQueryFunction.OUTPUT_FIELDS)
				.stateQuery(areaInfoState, AreaInfoQueryFunction.INPUT_FIELDS,
						new AreaInfoQueryFunction(),
						AreaInfoQueryFunction.OUTPUT_FIELDS)
				.stateQuery(resourceInfoState,
						ResourceQueryFunction.INPUT_FIELDS,
						new ResourceQueryFunction(),
						ResourceQueryFunction.OUTPUT_FIELDS)
				.each(AssemblySession.INPUT_FIELDS,
						new AssemblySession(),
						AssemblySession.OUTPUT_FIELDS)
				.project(AssemblySession.OUTPUT_FIELDS)
				.partitionBy(new Fields("sessionId"))
				.partitionPersist(new SessionStateFactory(),
						AssemblySession.OUTPUT_FIELDS,
						new SessionStateUpdater(),
						SessionStateUpdater.OUTPUT_FIELDS);
		hidTickState = sessionState
				.newValuesStream()
				.partitionBy(new Fields("hid"))
				.partitionPersist(new HidTickStateFactory(),
						HidTickStateUpdater.INPUT_FIELDS,
						new HidTickStateUpdater(),
						HidTickStateUpdater.OUTPUT_FIELDS);
		return hidTickState
				.newValuesStream()
				.aggregate(OnlineUserModifierCombiner.INPUT_FIELDS,
						new OnlineUserModifierCombiner(),
						OnlineUserModifierCombiner.OUTPUT_FIELDS)
				.stateQuery(onlineUserState,
						OnlineUserStateQueryUpdater.INPUT_FIELDS,
						new OnlineUserStateQueryUpdater(),
						OnlineUserStateQueryUpdater.OUTPUT_FIELDS);
	}

	public TridentState getOemInfoState() {
		return oemInfoState;
	}

	public void setOemInfoState(TridentState oemInfoState) {
		this.oemInfoState = oemInfoState;
	}

	public TridentState getAreaInfoState() {
		return areaInfoState;
	}

	public void setAreaInfoState(TridentState areaInfoState) {
		this.areaInfoState = areaInfoState;
	}

	public TridentState getResourceInfoState() {
		return resourceInfoState;
	}

	public void setResourceInfoState(TridentState resourceInfoState) {
		this.resourceInfoState = resourceInfoState;
	}

	public TridentState getOnlineUserState() {
		return onlineUserState;
	}

	public void setOnlineUserState(TridentState onlineUserState) {
		this.onlineUserState = onlineUserState;
	}

	public TridentState getSessionState() {
		return sessionState;
	}

	public void setSessionState(TridentState sessionState) {
		this.sessionState = sessionState;
	}

	public TridentState getHidTickState() {
		return hidTickState;
	}

	public void setHidTickState(TridentState hidTickState) {
		this.hidTickState = hidTickState;
	}

}
