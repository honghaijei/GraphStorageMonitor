package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;

/**
 * Created by hahong on 2016/8/3.
 */
public class TestTask implements AlgorithmTask {
    @Override
    public void prepare(GraphDataSet dataset) {
        System.out.println("prepare");
    }

    @Override
    public void run() {
        System.out.println("run");
    }
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("testtask", new TestTask());
        gpts.run();
    }
}
