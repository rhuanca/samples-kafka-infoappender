package edu.renidev.kafka;

public class ProductsPopulator {
	
	public static void main(String args[]) {
		String servers = "172.18.0.80:9092";
		String topic = "products";
		Utils.publishMessage(servers, topic, 0, "P1", "Product1Name,Product1Description");
		Utils.publishMessage(servers, topic, 1, "P2", "Product2Name,Product2Description");
		Utils.publishMessage(servers, topic, 0, "P3", "Product3Name,Product3Description");
		Utils.publishMessage(servers, topic, 1, "P4", "Product4Name,Product4Description");
		Utils.publishMessage(servers, topic, 0, "P5", "Product5Name,Product5Description");
		Utils.publishMessage(servers, topic, 1, "P6", "Product6Name,Product6Description");
		Utils.publishMessage(servers, topic, 0, "P7", "Product7Name,Product7Description");
		Utils.publishMessage(servers, topic, 1, "P8", "Product8Name,Product8Description");
	}
}
