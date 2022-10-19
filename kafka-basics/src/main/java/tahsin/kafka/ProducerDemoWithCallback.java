package tahsin.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

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

        final String topic = "transactions";



        // Send the record : Asynchronously


        while (true){
            // Generate a fake message
            FakeMessageGenerator messageGenerator = new FakeMessageGenerator();
            String key = messageGenerator.getRandomIntString();
            String message = messageGenerator.getFakeInformation();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info("Received new metadata : \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n" +
                                "Key : " + key + " ; "
                        );
                    }
                    else {
                        log.info("Error While Producing Message " + e);
                    }
                }
            });

            // flush the record : Synchronous

            producer.flush();

        }













    }
}
