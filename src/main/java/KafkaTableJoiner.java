import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class KafkaTableJoiner {

	public static String brokers = "10.100.0.121:9092";
	
	public static void main(String args[]) {
		
		Properties props = new Properties();
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/rhuanca/tmp/kafkastreams-store");
        
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KStreamBuilder builder = new KStreamBuilder();
		
		System.out.println("Loading ktable...");

		long t0 = System.currentTimeMillis();
		
		KStream<String, String> dataStream = builder.stream("data");
		KTable<String, String> table = builder.table("___cache1___", "___cache1___");

		long t1 = System.currentTimeMillis();
		System.out.println("t1: " + (t1-t0));
		
		dataStream.leftJoin(table, new ValueJoiner<String, String, String>() {

			@Override
			public String apply(String value1, String value2) {
				System.out.println("value1: " + value1);
				System.out.println("value2: " + value2);
				return value1 + "-" + value2;
			}
		}).to("result");
		
		long t2 = System.currentTimeMillis();
		System.out.println("t2: " + (t2-t1));
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        
        long t3 = System.currentTimeMillis();
        System.out.println("t3: " + (t3-t2));
        
	}
}
