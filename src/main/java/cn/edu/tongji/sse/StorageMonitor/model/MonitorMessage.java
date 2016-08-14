package cn.edu.tongji.sse.StorageMonitor.model;

/**
 * Created by hahong on 2016/7/6.
 */
public class MonitorMessage {
    public String machineId;
    public long createTime;
    public double CPUUsage;
    public long memoryUsage;
    public long networkSend;
    public long networkReceive;

    public String getMachineId() {
        return machineId;
    }

    public void setMachineId(String machineId) {
        this.machineId = machineId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public double getCPUUsage() {
        return CPUUsage;
    }

    public void setCPUUsage(double CPUUsage) {
        this.CPUUsage = CPUUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public long getNetworkSend() {
        return networkSend;
    }

    public void setNetworkSend(long networkSend) {
        this.networkSend = networkSend;
    }

    public long getNetworkReceive() {
        return networkReceive;
    }

    public void setNetworkReceive(long networkReceive) {
        this.networkReceive = networkReceive;
    }

    @Override
    public String toString() {
        return String.format("{MachineId: %s, CreateTime: %d, CPU: %f, Memory: %d, Network Send: %d, Network Receive: %d}", machineId, createTime, CPUUsage, memoryUsage, networkSend, networkReceive);
    }
}
