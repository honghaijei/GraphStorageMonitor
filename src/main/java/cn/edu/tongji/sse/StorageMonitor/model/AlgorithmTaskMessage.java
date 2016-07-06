package cn.edu.tongji.sse.StorageMonitor.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by hahong on 2016/7/6.
 */
public class AlgorithmTaskMessage implements Serializable {
    private String taskId;
    private String algorithm;
    private String storageEndpoint;
    private long createTime;
    private Map<String, String> parameters;

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

    @Override
    public String toString() {
        return String.format("{taskId: %s, algorithm: %s, storageEndpoint: %s, createTime: %d}", taskId, algorithm, storageEndpoint, createTime);
    }
}
