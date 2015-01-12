package com.voole.hobbit2.storm.test.merge;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SpoutTuples {
	public static enum SpoutType {
		DATA_SPOUT, TIMEOUT_SPOUT;
	}

	public static final Fields SPOUT_OUT_FIELDS = new Fields("spout_type",
			"spout_data");

	public static Values getDataValues(Object data) {
		return new Values(SpoutType.DATA_SPOUT, data);
	}

	public static Values getTimeoutValues() {
		return new Values(SpoutType.TIMEOUT_SPOUT);
	}

	public static class SpoutCustomStreamGrouping implements
			CustomStreamGrouping {
		private List<Integer> targetTasks;

		@Override
		public void prepare(WorkerTopologyContext context,
				GlobalStreamId stream, List<Integer> targetTasks) {
			this.targetTasks = targetTasks;
			for (Integer id : targetTasks) {
				System.out.println("taskid -----" + id);
			}
		}

		@Override
		public List<Integer> chooseTasks(int taskId, List<Object> values) {
			for (Integer id : targetTasks) {
				System.out.println("taskid -----" + id);
			}
			return targetTasks;
		}

	}
}
