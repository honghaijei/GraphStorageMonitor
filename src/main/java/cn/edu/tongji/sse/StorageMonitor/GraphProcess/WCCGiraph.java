package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j.Neo4jGraphDataSet;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by chenran on 2016/8/3 0003.
 */
public class WCCGiraph implements AlgorithmTask{
    /**input file name*/
    public static final String INPUT_NAME = "WCCInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/WCCInput";
    /*
    data format
    [VertexId<Tab>neighbor<Tab>neighbor.....
     */
    @Override
    public void prepare(GraphDataSet dataset) {
        System.out.println("start wcc giraph prepare");
        boolean isFirstEdge = true;
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                writer.write(nextNode.getId() + "");//vertex id
                //edges
                Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                while (edgesIterator.hasNext()){
                    GraphNode end = edgesIterator.next().getEnd();
                    writer.write("\t" + end.getId());
                }
                if(it.hasNext()){
                    writer.newLine();//换行
                }
            }
            System.out.println("finish wcc giraph prepare");
            writer.flush();
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /input/" + INPUT_NAME);
            System.out.println("finish wcc giraph rmr input");
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -put "+ INPUT_PATH  + " /input");
            System.out.println("finish wcc giraph put");
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /output/WCCOutput");
            System.out.println("finish wcc giraph rmr output");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                writer.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {

        Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop jar " +
                "~/giraph/giraph/giraph-examples/target/giraph-examples-1.2.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar " +
                "org.apache.giraph.GiraphRunner org.apache.giraph.examples.ConnectedComponentsComputation " +
                "-vif org.apache.giraph.io.formats.IntIntNullTextInputFormat " +
                "-vip /input/WCCInput -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat " +
                "-op /output/WCCOutput " +
                "-w 1 ");
        System.out.println("finish wcc giraph jar");
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.71", "192.168.1.72");
    }

    /*
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("wcc-giraph", new WCCGiraph());
        gpts.run();
        //GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        //WCCGiraph toyTask = new WCCGiraph();
        //toyTask.prepare(d);
        //toyTask.run();
    }
    */
}
