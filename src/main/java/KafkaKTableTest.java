import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.Logger;

public class KafkaKTableTest {
	private static Logger logger = Logger.getLogger(KafkaKTableTest.class);

	private static class Blinker implements Runnable {

		private String brokers;
		private String topic;
		private int partitions;
		private int maxMessages;
		private String preffixMessage;
		private int delay;

		public Blinker(String brokers, String topic, int partitions, int maxMessages, String preffixMessage, int delay) {
			this.brokers = brokers;
			this.topic = topic;
			this.partitions = partitions;
			this.maxMessages = maxMessages;
			this.preffixMessage = preffixMessage;
			this.delay = delay;
		}

		@Override
		public void run() {
			Properties props = new Properties();
			props.put("bootstrap.servers", brokers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<>(props);
			int counter = 0;
			for (int i = 0; i < maxMessages; i++) {
				int partition = counter % partitions;
				String key = String.valueOf(counter);
				String message = preffixMessage+" " + counter;

				logger.info("partition: " + partition);
				logger.info("key: " + key);
				logger.info("message: " + message);

				ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, message);
				producer.send(record);
				try {
					Thread.currentThread().sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				counter++;
			}
			producer.close();
		}
	}

	private static class Processor implements Runnable {

		private String brokers;
		private String dataTopic;
		private String tableTopic;
		private String resultTopic;

		public Processor(String brokers, String dataTopic, String tableTopic, String resultTopic) {
			this.brokers = brokers;
			this.dataTopic = dataTopic;
			this.tableTopic = tableTopic;
			this.resultTopic = resultTopic;
		}

		public void run() {
			Properties props = new Properties();
			props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor1");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
			props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			// props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/rhuanca/tmp/kafkastreams-store");

			KStreamBuilder builder = new KStreamBuilder();

			System.out.println("Loading ktable...");

			long t0 = System.currentTimeMillis();

			KStream<String, String> dataStream = builder.stream(dataTopic);
			KTable<String, String> table = builder.table(tableTopic, tableTopic);

			dataStream.leftJoin(table, new ValueJoiner<String, String, String>() {
				@Override
				public String apply(String value1, String value2) {
					System.out.println("value1: " + value1);
					System.out.println("value2: " + value2);
					return value1 + "-" + value2;
				}
			}).to("result");

			KafkaStreams streams = new KafkaStreams(builder, props);
			streams.start();

		}
	}

	public static void main(String args[]) {
		String brokers = "172.18.0.80:9092";

		ExecutorService executorService = Executors.newFixedThreadPool(10);

		executorService.execute(new Blinker(brokers, "table1", 5, 20, "message", 10));
		// executorService.execute(new Blinker(brokers, "topic2", 5, 20));
	}
}
