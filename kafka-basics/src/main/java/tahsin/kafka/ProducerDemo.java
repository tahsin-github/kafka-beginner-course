package tahsin.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) throws Exception {

        // Read the Kafka Server's ip address from the properties file
        Properties kafkaServerIPProperties = new Properties();
        InputStream is = new FileInputStream("kafka-basics/src/main/java/tahsin/properties/KafkaServer.properties");
        kafkaServerIPProperties.load(is);

        // Create Kafka Producer Properties
        Properties KafkaProperties = new Properties();

        KafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIPProperties.getProperty("bootstrap.servers"));
        KafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProperties);

        // Create a message/producer record

        final String topic = "transections";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "1", "500");

        // Send the record : Asynchronously

        producer.send(producerRecord);

        // flush the record : Synchronous

        producer.flush();

        // close the record

        producer.close();









    }
}
