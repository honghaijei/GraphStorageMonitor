package cn.edu.tongji.sse.StorageMonitor.Kafka;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Monitor.MonitorUtils;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmTaskMessage;
import cn.edu.tongji.sse.StorageMonitor.model.MonitorMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by honghaijie on 7/11/16.
 */
public class MonitorMessageProducer {

    public static void main(String[] args) throws IOException, InterruptedException {
        // MonitorMessageProducer producer = new MonitorMessageProducer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KafkaAddr);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 65536);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);


        double cpu = MonitorUtils.getCPUUsage();
        long memory = MonitorUtils.getMemoryUsage();
        MonitorUtils.NetworkIO networkSend = MonitorUtils.getNetWorkUsage();
        long in = networkSend.getReceive();
        long out = networkSend.getSend();
        System.out.printf("cpu: %f, memory: %dM, network receive: %d, network send: %d\n", cpu, memory, in, out);
        MonitorMessage msg = new MonitorMessage();
        msg.setCPUUsage(cpu);
        msg.setMemoryUsage(memory);
        msg.setNetworkSend(out);
        msg.setNetworkReceive(in);
        msg.setCreateTime(System.currentTimeMillis());
        msg.setMachineId(args[0]);

        byte[] bytes = Utils.writeKryoObject(msg);
        producer.send(new ProducerRecord<String, byte[]>(Config.KafkaMonitorMessageTopic, "", bytes));
        System.out.println(msg);
        producer.close();


    }
}
