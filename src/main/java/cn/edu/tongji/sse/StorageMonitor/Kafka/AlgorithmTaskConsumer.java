package cn.edu.tongji.sse.StorageMonitor.Kafka;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmTaskMessage;
import cn.edu.tongji.sse.StorageMonitor.model.ResultMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import java.util.Set;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by hahong on 2016/7/6.
 */
public class AlgorithmTaskConsumer {
    public static void main(String[] args) throws IOException{
        long startTime = System.currentTimeMillis();
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

        Properties res = new Properties();
        res.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KafkaAddr);
        res.put("acks", "all");
        res.put("retries", 0);
        res.put("batch.size", 1);
        res.put("linger.ms", 1);
        res.put("buffer.memory", 33554432);
        res.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        res.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(res);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10000);
            for (ConsumerRecord<String, byte[]> record : records) {
                AlgorithmTaskMessage msg = Utils.readKryoObject(AlgorithmTaskMessage.class, record.value());
                //System.out.println(msg.toString());

                //do pagerank
                String[] edges = {"0-1","0-1","0-2","0-3","1-2","1-3","2-0","2-1","3-2"};
                PageRankSingle prc = new PageRankSingle();
                prc.readFile(edges);
                PageRank<Integer, String> pr =
                        new PageRank<Integer, String>(prc.g, 0.15);
                pr.evaluate();
                Set<Integer> sortedVerticesSet =
                        new TreeSet<Integer>(prc.g.getVertices());

                //store msgRes
                ResultMessage msgRes = new ResultMessage();
                msgRes.setTaskId(msg.getTaskId());
                msgRes.setAlgorithm(msg.getAlgorithm());
                msgRes.setCreateTime(msg.getCreateTime());
                msgRes.setParameters(null);
                msgRes.setStorageEndpoint("");
                msgRes.setStartTime(startTime);
                for (Integer v : sortedVerticesSet) {
                    double score = pr.getVertexScore(v);
                    msgRes.addResult(v,score);
                }
                msgRes.setEndTime(System.currentTimeMillis());
                byte[] bytes = Utils.writeKryoObject(msgRes);
                producer.send(new ProducerRecord<String, byte[]>(Config.KafkaAlgorithmResultTopic, "", bytes));
                //System.out.println(msgRes);
            }
        }
    }
}
