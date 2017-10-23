package com.cpphot.msglevel;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.cpphot.action.Constants;

public class Producer {
	private final KafkaProducer<String, String> producer;
	private final String topic;

	public Producer(String topic, String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);// 16M
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);// 32M
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
		this.topic = topic;
	}

	public void producerMsg() throws InterruptedException {
		String data = "Apache Storm is a free and open source distributed realtime computation system Storm makes it easy to reliably process unbounded streams of data doing for realtime processing what Hadoop did for batch processing. Storm is simple, can be used with any programming language, and is a lot of fun to use!\n"
				+ "Storm has many use cases: realtime analytics, online machine learning, continuous computation, distributed RPC, ETL, and more. Storm is fast: a benchmark clocked it at over a million tuples processed per second per node. It is scalable, fault-tolerant, guarantees your data will be processed, and is easy to set up and operate.\n"
				+ "Storm integrates with the queueing and database technologies you already use. A Storm topology consumes streams of data and processes those streams in arbitrarily complex ways, repartitioning the streams between each stage of the computation however needed. Read more in the tutorial.";
		data = data.replaceAll("[\\pP‘’“”]", "");
		String[] words = data.split(" ");
		Random _rand = new Random();

		Random rnd = new Random();
		int events = 10;
		for (long nEvents = 0; nEvents < events; nEvents++) {
			long runtime = new Date().getTime();
			int lastIPnum = rnd.nextInt(255);
			String ip = "192.168.2." + lastIPnum;
			String msg = words[_rand.nextInt(words.length)];
			try {
				producer.send(new ProducerRecord<String, String>(topic, ip, msg));
				System.out.println("Sent message: (" + ip + ", " + msg + ")");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		Producer producer = new Producer("test_message_level", args);
		producer.producerMsg();
		System.out.println("发送数据成功");
		Thread.sleep(20);
	}
}
