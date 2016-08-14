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
public class SSSPGiraph implements AlgorithmTask{
    /**input file name*/
    public static final String INPUT_NAME = "SSSPInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/SSSPInput";
    /*
    data format
    [VertexId,VertexValue,[[TargetId,edgeValue],[TargetId,edgeValue].....]]
     */
    @Override
    public void prepare(GraphDataSet dataset) {
        System.out.println("start SSSP giraph prepare");
        boolean isFirstEdge;
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                if (nextNode.getOutEdges().size() != 0){
                    isFirstEdge = true;
                    writer.write("[");
                    writer.write(nextNode.getId() + ",0,");//vertex id and value
                    //edges
                    writer.write("[");
                    Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                    Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                    while (edgesIterator.hasNext()){
                        if(isFirstEdge == true){
                            GraphNode end = edgesIterator.next().getEnd();
                            writer.write("[" + end.getId() + ",1]");
                            isFirstEdge = false;
                        }
                        else{
                            writer.write(",");
                            GraphNode end = edgesIterator.next().getEnd();
                            writer.write("[" + end.getId() + ",1]");
                        }
                    }
                    writer.write("]]");
                    if(it.hasNext()){
                        writer.newLine();//换行
                    }
                }
            }
            System.out.println("finish SSSP giraph prepare");
            writer.flush();
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /input/" + INPUT_NAME);
            System.out.println("finish SSSP giraph rmr input");
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -put " + INPUT_PATH + " /input");
            System.out.println("finish sssp giraph put");
            Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /output/SSSPOutput");
            System.out.println("finish sssp giraph rmr output");
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
                "org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimpleShortestPathsComputation " +
                "-vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat " +
                "-vip /input/SSSPInput -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat " +
                "-op /output/SSSPOutput " +
                "-w 1 ");
        System.out.println("finish sssp giraph jar");
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.71", "192.168.1.72");
    }

    /*
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("sssp-giraph", new SSSPGiraph());
        gpts.run();
        //GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        //SSSPGiraph toyTask = new SSSPGiraph();
        //toyTask.prepare(d);
        //toyTask.run();
    }
    */
}
