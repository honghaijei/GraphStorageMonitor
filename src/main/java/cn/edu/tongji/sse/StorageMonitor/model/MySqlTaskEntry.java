package cn.edu.tongji.sse.StorageMonitor.model;

/**
 * Created by hahong on 2016/8/18.
 */
public class MySqlTaskEntry {
    public String taskid;
    public String taskDescription;
    public String taskType;
    public long createtime;
    public String status;

    public MySqlTaskEntry(String taskid, String taskDescription, String taskType, long createtime, String status) {
        this.taskid = taskid;
        this.taskDescription = taskDescription;
        this.taskType = taskType;
        this.createtime = createtime;
        this.status = status;
    }

    @Override
    public String toString() {
        return "MySqlTaskEntry{" +
                "taskid='" + taskid + '\'' +
                ", taskDescription='" + taskDescription + '\'' +
                ", taskType='" + taskType + '\'' +
                ", createtime=" + createtime +
                ", status='" + status + '\'' +
                '}';
    }
}
