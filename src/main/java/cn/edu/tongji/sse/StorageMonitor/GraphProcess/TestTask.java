package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;

import java.util.Arrays;
import java.util.Collection;

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
        try {
            Thread.sleep(1000 * 20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.71", "192.168.1.72", "192.168.1.73");
    }

    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("testtask", new TestTask());
        gpts.run();
    }
}
