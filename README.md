1. Create topics in kafka brokers:
   Make sure to use appropiate ip addresses

   bin/kafka-topics.sh --zookeeper 172.18.0.70:2181 --create --topic products --partitions 2 --replication-factor 1
   bin/kafka-topics.sh --zookeeper 172.18.0.70:2181 --create --topic customers --partitions 3 --replication-factor 1 
   bin/kafka-topics.sh --zookeeper 172.18.0.70:2181 --create --topic purchases --partitions 5 --replication-factor 1

2. Execute through a java IDE the following classes:

   - CustomersPopulator
   - ProductsPopulator

   These should populate data in the kafka topics. Also make sure to set appropiate addresses in java classes

3. Execute through a java IDE the main processor class:

   - PurchasesProcessor

   This should start a deamon and start waiting for messages in the kafka messages in the purchases topic.

4. To send a purchase message execute:
   
   echo 'p0001-customer:C01,product:P1' | bin/kafka-console-producer.sh --broker-list 172.18.0.80:9092 --topic purchases --property parse.key=true --property key.separator=-
   
   
