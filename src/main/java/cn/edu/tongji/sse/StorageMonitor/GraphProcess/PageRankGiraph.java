package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j.Neo4jGraphDataSet;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by chenran on 2016/8/2 0002.
 */
public class PageRankGiraph implements AlgorithmTask{
    /**input file name*/
    public static final String INPUT_NAME = "PageRankInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/PageRankInput";
    /*
    data format
    [VertexId,VertexValue,[[TargetId,edgeValue],[TargetId,edgeValue].....]]
     */
    @Override
    public void prepare(GraphDataSet dataset) {
        System.out.println("start page giraph prepare");
        boolean isFirstEdge = true;
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()) {
                GraphNode nextNode = it.next();
                if (nextNode.getOutEdges().size() != 0) {
                    isFirstEdge = true;
                    writer.write("[");
                    writer.write(nextNode.getId() + ",0,");//vertex id and value
                    //edges
                    writer.write("[");
                    Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                    Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                    while (edgesIterator.hasNext()) {
                        if (isFirstEdge == true) {
                            GraphNode end = edgesIterator.next().getEnd();
                            writer.write("[" + end.getId() + ",1]");
                            isFirstEdge = false;
                        } else {
                            writer.write(",");
                            GraphNode end = edgesIterator.next().getEnd();
                            writer.write("[" + end.getId() + ",1]");
                        }
                    }
                    writer.write("]]");
                    if (it.hasNext()) {
                        writer.newLine();//换行
                    }
                }
            }
            System.out.println("finish page giraph prepare");
            writer.flush();
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
        String pwdString = Execute.exec("pwd").toString();
        System.out.println("finish page giraph pwd");
        Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /input/" + INPUT_NAME);
        System.out.println("finish page giraph rmr input");
        Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -put " + INPUT_PATH  + " /input");
        System.out.println("finish page giraph put");
        Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr /output/PageRankOutput");
        System.out.println("finish page giraph rmr output");
        Execute.exec("~/hadoop/hadoop-0.20.203.0/bin/hadoop jar " +
                "~/giraph/giraph/giraph-examples/target/giraph-examples-1.2.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar " +
                "org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimplePageRankComputation " +
                "-vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat " +
                "-vip /input/PageRankInput -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat " +
                "-op /output/PageRankOutput " +
                "-w 1 " +
                "-mc org.apache.giraph.examples.SimplePageRankComputation\\$SimplePageRankMasterCompute");
        System.out.println("finish page giraph jar");
        String lsString = Execute.exec("ls -l").toString();

        System.out.println("==========INFO=============");
        System.out.println(pwdString);
        System.out.println(lsString);
    }

    @Override
    public Collection<String> getMachines() {
        return null;
    }

    public static void main(String[] args) {
        //GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        //gpts.addTask("pagerank-giraph", new DMSTGiraph());
        //gpts.run();
        GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        PageRankGiraph toyTask = new PageRankGiraph();
        toyTask.prepare(d);
        toyTask.run();
    }
}
