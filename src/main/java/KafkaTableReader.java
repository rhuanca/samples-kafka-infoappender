import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class KafkaTableReader {

	public static String brokers = "10.100.0.121:9092";
	
	public static void main(String args[]) throws InterruptedException {
		
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount11");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KStreamBuilder builder = new KStreamBuilder();
		
		System.out.println("Loading ktable...");
		long t0 = System.currentTimeMillis();
		KTable<Object, Object> table = builder.table("cache", "cache");
		long t1 = System.currentTimeMillis();
		System.out.println("table.getStoreName():" + table.getStoreName());
		System.out.println("total time: " + (t1 - t0));
		
		long t2 = System.currentTimeMillis();
		KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        long t3 = System.currentTimeMillis();
        System.out.println("total time new: " + (t3 - t2));
        
        while(true);
        //Thread.sleep(5000L);

        //streams.close();
	}
}
