package edu.renidev.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class PurchasesProcessor {

	public static void main(String args[]) {
		String brokers = "172.18.0.80:9092" + "";
		Properties props = new Properties();
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchases-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// props.put(StreamsConfig.STATE_DIR_CONFIG,
		// "/home/rhuanca/tmp/kafkastreams-store");
		// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> purchasesStream = builder.stream("purchases");
		KTable<String, String> productsTable = builder.table("products", "products");
		KTable<String, String> customersTable = builder.table("customers", "customers");

		purchasesStream.foreach(new ForeachAction<String, String>() {
			@Override
			public void apply(String key, String value) {
				System.out.println("purchase received");
				System.out.println("key:" + key);
				String[] records = value.split(",");
				String customer = records[0].split(":")[1];
				String product = records[1].split(":")[1];
				System.out.println("customer: " + customer);
				System.out.println("product: " + product);
			}
		});

		purchasesStream.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {

			@Override
			public KeyValue<String, String> apply(String key, String value) {
				String[] records = value.split(",");
				String customer = records[0].split(":")[1];
				// String product = records[1].split(":")[1]; // not used here
				return new KeyValue<String, String>(customer, value);
			}
		}).leftJoin(customersTable, new ValueJoiner<String, String, String>() {
			@Override
			public String apply(String purchase, String customer) {
				String customerName = customer.split(",")[1];
				return purchase + " linked to customer: " + customerName;
			}
		}).foreach(new ForeachAction<String, String>() {
			@Override
			public void apply(String key, String value) {
				System.out.println("value: " + value);
			}
		});

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
	}
}
