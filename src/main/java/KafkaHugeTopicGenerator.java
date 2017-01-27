import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaHugeTopicGenerator {
	// private static String servers = "172.18.0.80:9092";
	private static String servers = "10.100.0.121:9092";
	private static long MAX = 10000000;

	public static void main(String args[]) {
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		System.out.println("Sending....");
		for (long i = 0; i < MAX; i++) {
			producer.send(new ProducerRecord<String, String>("___cache1___", Long.toString(i), "cached_message_" + i));
		}
		System.out.println("finished to send.");
		producer.close();

	}
}
