package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;

/**
 * Created by hahong on 2016/7/29.
 */
public class MapReduce implements AlgorithmTask {
    @Override
    public AlgorithmResultMessage run(GraphDataSet dataset) {
        AlgorithmResultMessage msg = new AlgorithmResultMessage();
        msg.setAlgorithm("mapreduce");
        msg.setCreateTime(System.currentTimeMillis() - 1000);
        msg.setStartTime(System.currentTimeMillis() - 500);
        msg.setEndTime(System.currentTimeMillis());

        return msg;
    }
}
