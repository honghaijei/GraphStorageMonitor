package cn.edu.tongji.sse.StorageMonitor.Kafka;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmTaskMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by hahong on 2016/7/6.
 */
public class AlgorithmTaskConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KafkaAddr);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Arrays.asList(Config.KafkaAlgorithmTaskTopic));
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10000);
            for (ConsumerRecord<String, byte[]> record : records) {
                AlgorithmTaskMessage msg = Utils.readKryoObject(AlgorithmTaskMessage.class, record.value());
                System.out.println(msg.toString());
            }
        }
    }
}
