package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
/**
 * Created by chenran on 2016/8/2 0002.
 */
public class SSSP implements AlgorithmTask{
    @Override
    public void prepare(GraphDataSet dataset) {
        System.out.println("prepare sssp!");
    }

    @Override
    public void run() {
        System.out.println("run sssp!");
    }
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.setTask("abc", new SSSP());
        gpts.run();
    }
}
