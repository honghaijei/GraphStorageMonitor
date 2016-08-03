package cn.edu.tongji.sse.StorageMonitor;

import java.io.Serializable;

/**
 * Created by hahong on 2016/7/6.
 */
public class Config implements Serializable {
    public static String KafkaAddr = "10.60.45.80:9092";
    public static String KafkaInternalAddr = "192.168.1.233:9092";
    public static String KafkaAlgorithmResultTopic = "Result_debug";
    public static String KafkaMonitorMessageTopic = "Monitor_debug";

    public static String KafkaAlgorithmPageRankTaskTopic = "PageRank_";

    public static String AlgorithmPageRank = "PageRank";
    public static String AlgorithmSSSP = "SSSP";
}
