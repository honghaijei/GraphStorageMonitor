package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.model.AlgorithmResultMessage;

/**
 * Created by hahong on 2016/7/29.
 */
public class PageRank implements AlgorithmTask {
    @Override
    public void run(GraphDataSet dataset) {
        long sum = 0;
        for (int i = 0; i < 10000000; ++i) {
            sum += i;
        }
        System.out.println(sum);
    }
}
