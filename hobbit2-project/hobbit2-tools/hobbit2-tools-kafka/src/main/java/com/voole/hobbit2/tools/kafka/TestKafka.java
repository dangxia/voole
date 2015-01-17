package com.voole.hobbit2.tools.kafka;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.I0Itec.zkclient.ZkClient;

import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

public class TestKafka {
	public static void main(String[] args) throws InterruptedException {
		ZkClient zkClient = ZookeeperUtils.createZKClient(
				"dev-test1:2181,dev-test2:2181,dev-test3:2181/kafka", 40000,
				40000);
		List<BrokerAndTopicPartition> paritions = KafkaUtils.getPartitions2(
				zkClient, "test1");
		for (BrokerAndTopicPartition brokerAndTopicPartition : paritions) {
			System.out.println(brokerAndTopicPartition);
		}

		long events = 220;
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list",
				"dev-test1:9092,dev-test2:9092,dev-test3:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			TimeUnit.SECONDS.sleep(1);
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			String msg = runtime + ",www.example.com," + ip;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"page_visits", msg, msg);
			producer.send(data);
		}
		producer.close();
	}
}
