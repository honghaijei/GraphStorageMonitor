package cn.edu.tongji.sse.StorageMonitor.Monitor;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Helper.MonitorLogDatabaseConnector;
import cn.edu.tongji.sse.StorageMonitor.LRUCache;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;
import cn.edu.tongji.sse.StorageMonitor.model.MonitorMessage;
import cn.edu.tongji.sse.StorageMonitor.model.MySqlAllDataEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by hahong on 2016/8/3.
 */
public class MonitorDataStorage {
    public ConcurrentMap<String, LRUCache<Long, MonitorMessage>> cache = new ConcurrentHashMap<>(1000);
    public final static String CONSUME_GROUP = "mysql_t";
    public MonitorDataStorage() {
        Thread consumeMonitorDataThread = new Thread() {
            public void run() {
                getMonitorData();
            }
        };
        consumeMonitorDataThread.start();
    }
    public void getMonitorData() {
        Properties result = new Properties();
        result.put("bootstrap.servers", Config.KafkaInternalAddr);
        result.put("group.id", CONSUME_GROUP);
        result.put("enable.auto.commit", "true");
        result.put("auto.commit.interval.ms", "1000");
        result.put("session.timeout.ms", "30000");
        result.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        result.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(result);
        consumer.subscribe(Arrays.asList(Config.KafkaMonitorMessageTopic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10000);
            for (ConsumerRecord<String, byte[]> record : records) {
                MonitorMessage msg = Utils.readKryoObject(MonitorMessage.class, record.value());
                synchronized (cache) {
                    if (!cache.containsKey(msg.getMachineId())) {
                        cache.put(msg.getMachineId(), new LRUCache<Long, MonitorMessage>(3600 * 24 * 2));
                    }
                    System.out.printf("Get monitor message from %s, time: %d\n", msg.getMachineId(), msg.getCreateTime());
                    cache.get(msg.getMachineId()).put(msg.getCreateTime(), msg);
                }
            }
        }
    }
    public void run() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", Config.KafkaInternalAddr);
        prop.put("group.id", CONSUME_GROUP);
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(prop);
        consumer.subscribe(Arrays.asList(Config.KafkaAlgorithmResultTopic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10000);
            for (ConsumerRecord<String, byte[]> record : records) {
                AlgorithmResultMessage msg = Utils.readKryoObject(AlgorithmResultMessage.class, record.value());
                System.out.printf("Get result message, taskid: %s\n", msg.getTaskId());
                List<MySqlAllDataEntry> buf = new ArrayList<>();
                synchronized (cache) {
                    for (String machine : msg.getMachines()) {
                        LRUCache<Long, MonitorMessage> logs = cache.get(machine);
                        if (logs == null) {
                            System.out.println("WARNING: wrong machine id or machine has die.");
                            continue;
                        }

                        try {
                            JSONObject json = new JSONObject();
                            JSONArray jsonLogs = new JSONArray();
                            for (Map.Entry<Long, MonitorMessage> e : logs.entrySet()) {
                                if (e.getKey() >= msg.getStartTime() && e.getKey() < msg.getEndTime()) {
                                    JSONObject t = new JSONObject();
                                    t.put("cpu", e.getValue().getCPUUsage() + "");
                                    t.put("memory", e.getValue().getMemoryUsage() + "");
                                    t.put("networkReceive", e.getValue().getNetworkReceive() + "");
                                    t.put("networkSend", e.getValue().getNetworkSend() + "");
                                    jsonLogs.put(t);
                                }
                            }
                            json.put("data", jsonLogs);
                            MySqlAllDataEntry dataEntry = new MySqlAllDataEntry(msg.getTaskId(), machine, msg.getStartTime(), msg.getEndTime(), json.toString());
                            buf.add(dataEntry);
                            System.out.println(dataEntry.toString());
                            MonitorLogDatabaseConnector connector = new MonitorLogDatabaseConnector();
                            Connection c = connector.ConnectMysql();
                            for (MySqlAllDataEntry e : buf) {
                                connector.InsertSql(e);
                            }
                            try {
                                connector.CutConnection(c);
                            } catch (SQLException e1) {
                                e1.printStackTrace();
                            }
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
    }
    public static void main(String args[]) {
        MonitorDataStorage mds = new MonitorDataStorage();
        mds.run();
    }
}
