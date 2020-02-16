package com.qi;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {

	private static Properties properties = new Properties();
	private static KafkaProducer<String, String> producer;

	static {
		properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");

		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(properties);

	}

	public static void sendMsg(String key, String value) {

		// 会根据传入的key自动分区，传入key为空或相同时消息不分区，会传到同一个partition中,
		// ProducerRecord<String, String> record = new ProducerRecord<>("bd20test1",key, value);
		
		// 将消息发送到指定的partition上
		ProducerRecord<String, String> record = new ProducerRecord<>("bd20test2", 0, key, value);

		producer.send(record);
	}

	public static void close() {
		if (producer != null) {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 50; i++) {
			// try {
			// Thread.sleep(1000);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			sendMsg("key " + i, "value " + i);
		}
		close();
	}

}
