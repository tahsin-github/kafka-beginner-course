package tahsin.ConsumerGroup;



import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithConsumerGroupConsumerOne {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithConsumerGroupConsumerOne.class.getName());

    public static void main(String[] args) throws Exception {

        final String groupId = "kafka-application.three";
        final String topic = "transactions";

        // Read the Kafka Server's ip address from the properties file
        Properties kafkaServerIPProperties = new Properties();
        InputStream is = new FileInputStream("kafka-basics/src/main/java/tahsin/properties/KafkaServer.properties");
        kafkaServerIPProperties.load(is);

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIPProperties.getProperty("bootstrap.servers"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();


        // Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown Detected. Exit by calling consumer.wakeup()");
                consumer.wakeup();

                /* Join the main thread to allow the execution of the code in the main thread */
                try {
                    mainThread.join();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });



        try {
            // Subscribe the consumer to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll for the messages
            while (true) {
                log.info("Polling messages ... ");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() + " value : " + record.value());
                    log.info("Partition : " + record.partition() + " Offset : " + record.offset());
                }

            }
        }
        catch (WakeupException e){
            log.info("Wakeup Exception ! ");
        }
        catch (Exception e) {
            log.error("Unexpected Error");
        }
        finally {
            consumer.close();
            log.info("Consumer is Gracefully Closed.");
        }


    }
}

