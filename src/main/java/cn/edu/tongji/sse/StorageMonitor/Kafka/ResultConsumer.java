package cn.edu.tongji.sse.StorageMonitor.Kafka;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;
/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class ResultConsumer {
    public static void main(String[] args){
        Properties result = new Properties();
        result.put("bootstrap.servers", Config.KafkaInternalAddr);
        result.put("group.id", "test");
        result.put("enable.auto.commit", "true");
        result.put("auto.commit.interval.ms", "1000");
        result.put("session.timeout.ms", "30000");
        result.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        result.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(result);
        consumer.subscribe(Arrays.asList(Config.KafkaAlgorithmResultTopic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10000);
            for (ConsumerRecord<String, byte[]> record : records) {
                AlgorithmResultMessage msg = Utils.readKryoObject(AlgorithmResultMessage.class, record.value());
                System.out.println(msg.toString());
            }
        }
    }
}
