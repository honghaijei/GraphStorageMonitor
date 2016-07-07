package cn.edu.tongji.sse.StorageMonitor.model;

import org.apache.commons.collections15.map.LinkedMap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by Administrator on 2016/7/6 0006.
 */
public class ResultMessage implements Serializable {
    private String taskId;
    private String algorithm;
    private String storageEndpoint;
    private long createTime;
    private Map<String, String> parameters;

    private long startTime;
    private long endTime;
    private Map<String,String> result = new LinkedMap<String, String>();

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getStorageEndpoint() {
        return storageEndpoint;
    }

    public void setStorageEndpoint(String storageEndpoint) {
        this.storageEndpoint = storageEndpoint;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public void setStartTime (long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime (long endTime) {
        this.endTime = endTime;
    }

    public void addResult(int v, double score){
        this.result.put(String.valueOf(v), String.valueOf(score));
    }

    @Override
    public String toString() {
        String resultStr = "";
        for(int i=0; i<result.size();i++){
            resultStr += i + ": " + result.get(String.valueOf(i)) + "; ";
        }
        return String.format("{taskId: %s, algorithm: %s, storageEndpoint: %s, result: %s,createTime: %d, startTime: %d, endTime: %d}", taskId, algorithm, storageEndpoint, resultStr,createTime, startTime, endTime );
    }
}
