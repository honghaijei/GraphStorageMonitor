package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

/**
 * Created by Administrator on 2016/8/8 0008.
 */
public class GiraphRunner {
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("DMST Giraph", new DMSTGiraph());
        gpts.addTask("PageRank Giraph", new PageRankGiraph());
        gpts.addTask("SSSP Giraph", new SSSPGiraph());
        gpts.addTask("WCC Giraph", new WCCGiraph());
        gpts.run();
    }
}
