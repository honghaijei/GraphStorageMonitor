package cn.edu.tongji.sse.StorageMonitor.model;

/**
 * Created by hahong on 2016/8/3.
 */
public class MySqlAllDataEntry {
    public String jobid;
    public String machineId;
    public long startTime;
    public long endTime;
    public String json;

    @Override
    public String toString() {
        return "MySqlAllDataEntry{" +
                "jobid='" + jobid + '\'' +
                ", machineId='" + machineId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", json='" + json + '\'' +
                '}';
    }

    public MySqlAllDataEntry(String jobid, String machineId, long startTime, long endTime, String json) {
        this.jobid = jobid;
        this.machineId = machineId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.json = json;
    }

    public void persist() {

    }
}
