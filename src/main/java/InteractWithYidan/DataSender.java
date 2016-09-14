package InteractWithYidan;

import cn.edu.tongji.sse.StorageMonitor.Config;
import cn.edu.tongji.sse.StorageMonitor.Utils;
import cn.edu.tongji.sse.StorageMonitor.model.MonitorMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hahong on 2016/9/11.
 */
public class DataSender {
    private BlockingQueue<MonitorMessage> q = new ArrayBlockingQueue<MonitorMessage>(1000);
    private void consumeThread() {
        Properties result = new Properties();
        result.put("bootstrap.servers", Config.KafkaInternalAddr);
        result.put("group.id", "test");
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
                try {
                    q.put(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private void startConsumeThread() {
        Thread t = new Thread() {
            public void run() {
                consumeThread();
            }
        };
        t.start();
    }
    private String fakeData() {
        try {
            MonitorMessage msg = q.take();
            try {
                JSONObject jo = new JSONObject();
                jo.accumulate("cpu", msg.getCPUUsage());
                jo.accumulate("networkSend", msg.getNetworkSend());
                jo.accumulate("networkReceive", msg.getNetworkReceive());
                jo.accumulate("memory", msg.getMemoryUsage());
                jo.accumulate("createTime",
                        msg.getCreateTime());
                JSONObject jojori = new JSONObject();
                jojori.accumulate("data", jo);
                jojori.accumulate("machineid", msg.getMachineId());
                return jojori.toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return "{}";
    }

    static public void main(String[] args) {


        String uu = "http://localhost:8080/ClusterData/servlet/AddRealTimeData";
        DataSender ds = new DataSender();
        ds.startConsumeThread();
        while (true) {
            String dt = ds.fakeData();
            System.out.println("Send by qyd: " + dt);
            HttpRequester.httpPostWithJSON(uu, dt);
        }
    }
}
