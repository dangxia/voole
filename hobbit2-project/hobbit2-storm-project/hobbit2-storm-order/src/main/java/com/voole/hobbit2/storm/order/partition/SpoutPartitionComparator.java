/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

import storm.trident.spout.ISpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class SpoutPartitionComparator implements Comparator<ISpoutPartition> {

	@Override
	public int compare(ISpoutPartition o1, ISpoutPartition o2) {
		if (o1.getClass() != o2.getClass()) {
			if (o1.getClass() == GCSpoutPartition.class) {
				return 1;
			}
			return -1;
		} else {
			return ((KafkaSpoutPartition) o1)
					.compareTo((KafkaSpoutPartition) o2);
		}
	}

	public static void main(String[] args) {
		List<ISpoutPartition> list = new ArrayList<ISpoutPartition>();
		list.add(new GCSpoutPartition());

		BrokerAndTopicPartition brokerAndTopicPartition = new BrokerAndTopicPartition(
				new Broker("", 9, 1), "test", 1);
		KafkaSpoutPartition p1 = new KafkaSpoutPartition();
		p1.setBrokerAndTopicPartition(brokerAndTopicPartition);
		list.add(p1);

		brokerAndTopicPartition = new BrokerAndTopicPartition(new Broker("", 9,
				2), "test", 1);
		p1 = new KafkaSpoutPartition();
		p1.setBrokerAndTopicPartition(brokerAndTopicPartition);
		list.add(p1);

		brokerAndTopicPartition = new BrokerAndTopicPartition(new Broker("", 9,
				1), "1test", 1);
		p1 = new KafkaSpoutPartition();
		p1.setBrokerAndTopicPartition(brokerAndTopicPartition);
		list.add(p1);

		brokerAndTopicPartition = new BrokerAndTopicPartition(new Broker("", 9,
				1), "1test", 10);
		p1 = new KafkaSpoutPartition();
		p1.setBrokerAndTopicPartition(brokerAndTopicPartition);
		list.add(p1);

		Collections.sort(list, new SpoutPartitionComparator());
		for (ISpoutPartition iSpoutPartition : list) {
			System.out.println(iSpoutPartition);
		}
	}

}
