package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

/**
 * Created by Administrator on 2016/8/8 0008.
 */
public class HadoopRunner {
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("DMST", new DMST());
        gpts.addTask("PageRank", new PageRank());
        gpts.addTask("SSSP", new SSSP());
        gpts.addTask("WCC", new WCC());
        gpts.run();
        //GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        //DMST toyTask = new DMST();
        //toyTask.prepare(d);
        //toyTask.run();
    }
}
