package cn.edu.tongji.sse.StorageMonitor.Kafka;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Helper.AlgorithmTaskDatabaseConnector;
import cn.edu.tongji.sse.StorageMonitor.Helper.MonitorLogDatabaseConnector;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmTaskMessage;
import cn.edu.tongji.sse.StorageMonitor.model.MySqlAllDataEntry;
import cn.edu.tongji.sse.StorageMonitor.model.MySqlTaskEntry;
import org.apache.kafka.clients.producer.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by hahong on 2016/7/6.
 */
public class AlgorithmTaskProducer {
    private static Random rand = new Random();
    private static int count = 1;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KafkaInternalAddr);

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
        String[] choices = { Config.AlgorithmPageRank, Config.AlgorithmSSSP };
        for (int i = 0; i < count; ++i) {
            AlgorithmTaskMessage msg = new AlgorithmTaskMessage();
            msg.setTaskId(java.util.UUID.randomUUID().toString());
            msg.setAlgorithm(choices[rand.nextInt(choices.length)]);
            msg.setCreateTime(System.currentTimeMillis());
            msg.setParameters(null);
            msg.setStorageEndpoint("");
            byte[] bytes = Utils.writeKryoObject(msg);

            AlgorithmTaskDatabaseConnector connector = new AlgorithmTaskDatabaseConnector();
            Connection c = connector.ConnectMysql();
            connector.InsertSql(new MySqlTaskEntry(msg.getTaskId(), msg.getAlgorithm(), args[0], msg.getCreateTime(), "waiting"));
            try {
                connector.CutConnection(c);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            producer.send(new ProducerRecord<String, byte[]>(args[0], "", bytes));
            System.out.println(msg);
        }

        producer.close();
    }
}
