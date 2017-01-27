package edu.renidev.kafka;

public class CustomersPopulator {
	public static void main(String args[]) {
		String servers = "172.18.0.80:9092";
		String topic = "customers";
		Utils.publishMessage(servers, topic, 0, "C01", "Customer1Name,Customer1Description");
		Utils.publishMessage(servers, topic, 1, "C02", "Customer2Name,Customer2Description");
		Utils.publishMessage(servers, topic, 2, "C03", "Customer3Name,Customer3Description");
		Utils.publishMessage(servers, topic, 0, "C04", "Customer4Name,Customer4Description");
		Utils.publishMessage(servers, topic, 1, "C05", "Customer5Name,Customer5Description");
		Utils.publishMessage(servers, topic, 2, "C06", "Customer6Name,Customer6Description");
		Utils.publishMessage(servers, topic, 0, "C07", "Customer7Name,Customer7Description");
		Utils.publishMessage(servers, topic, 1, "C08", "Customer8Name,Customer8Description");
		Utils.publishMessage(servers, topic, 2, "C09", "Customer9Name,Customer9Description");
	}

}
