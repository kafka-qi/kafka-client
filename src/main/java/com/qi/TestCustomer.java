package com.qi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class TestCustomer {

	private static Properties props = new Properties();
	private static KafkaConsumer<String, String> consumer;

	static {
		props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "none");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
	}

	public static void receiveMessage() {

		String topic = "bd20test2";
		//接收指定分区的消息
		TopicPartition partition0 = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partition0));
		
		//接收所有消息
//		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}
	
	
	public static void close() {
		if (consumer != null) {
			consumer.close();
		}
	}
	
	public static void assignPartition(String topic,int partitionNo,long offset) {
		
		TopicPartition topicPartition=new TopicPartition(topic, partitionNo);
		List<TopicPartition> topicPartitions=new ArrayList<>();
		topicPartitions.add(topicPartition);
		
		consumer.assign(topicPartitions);
		consumer.seek(topicPartition, offset);
		
		while (true) {
			ConsumerRecords<String, String> records=consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				String key = record.key();
				String value = record.value();
				int partitionNoa = record.partition();
				long offseta = record.offset();
				long timestamp = record.timestamp();
				System.out.println("key:"+key+",value:"+value+",partitionNo:"+partitionNoa+",offset:"+offseta+",timestamp:"+timestamp);

			}
		}
	}
	
	//手动提交offset并改变offset的值
	
	public static void updateConsumerOffset() {
		TopicPartition topicPartition=new TopicPartition("bd20test2", 0);
		List<TopicPartition> partitions=new ArrayList<>();
		
		partitions.add(topicPartition);
		consumer.assign(partitions);
		Long begin =null;
		ConsumerRecords<String, String> reConsumerRecords=consumer.poll(1000);
		
		for (ConsumerRecord<String, String> record : reConsumerRecords) {
			if (begin==null) {
				begin=record.offset();
				System.out.println("消费从"+begin+"开始");
			}
			System.out.printf("offset=%d,key=%s,value=%s%n",record.offset(),record.key(),record.value());
			
		}
		Map<TopicPartition, OffsetAndMetadata> commitOffset=new HashMap<>();
		OffsetAndMetadata offsetAndMetadata=new OffsetAndMetadata(10);
		commitOffset.put(topicPartition, offsetAndMetadata);
		
		consumer.commitAsync(commitOffset, new OffsetCommitCallback() {
			
			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

				System.out.println("服务端已经成功地将offset更新为10");
			}
		});
		
		consumer.commitSync(commitOffset);
		consumer.close();
	}
	
	public static void main(String[] args) {
//		receiveMessage();
//		assignPartition("bd20test2", 0, 9);
//		close();
		updateConsumerOffset();
	}


}
