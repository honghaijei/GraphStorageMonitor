package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
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
        boolean isFirstEdge = true;
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File("INPUT_PATH");
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                writer.write("[");
                GraphNode nextNode = it.next();
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
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -rmr /input/INPUT_NAME");
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -put INPUT_PATH /input");
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -rmr /output/PageRankOutput");
        Execute.exec("/usr/local/hadoop/./bin/hadoop jar " +
                "$GIRAPH_HOME/giraph-examples/target/giraph-examples-1.2.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar " +
                "org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimplePageRankComputation " +
                "-vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat " +
                "-vip /input/PageRankInput -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat " +
                "-op /output/PageRankOutput " +
                "-w 1 " +
                "-mc org.apache.giraph.examples.SimplePageRankComputation\\$SimplePageRankMasterCompute");
        String lsString = Execute.exec("ls -l").toString();

        System.out.println("==========INFO=============");
        System.out.println(pwdString);
        System.out.println(lsString);
    }
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.setTask("pagerank-giraph", new DMSTGiraph());
        gpts.run();
    }
}
