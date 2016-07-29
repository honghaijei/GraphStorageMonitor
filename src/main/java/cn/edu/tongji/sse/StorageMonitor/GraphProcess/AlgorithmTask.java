package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;

/**
 * Created by hahong on 2016/7/29.
 */
public interface AlgorithmTask {
    AlgorithmResultMessage run(GraphDataSet dataset);
}
