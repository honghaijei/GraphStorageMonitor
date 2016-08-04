package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;

import java.util.Collection;

/**
 * Created by hahong on 2016/7/29.
 */
public interface AlgorithmTask {
    void prepare(GraphDataSet dataset);
    void run();
    Collection<String> getMachines();
}
