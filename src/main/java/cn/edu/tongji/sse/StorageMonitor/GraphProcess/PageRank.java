package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j.Neo4jGraphDataSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by hahong on 2016/7/29.
 */
public class PageRank implements AlgorithmTask {

    /**input file name*/
    public static final String INPUT_NAME = "PageRankInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/PageRankInput";
    @Override
    /*
    format:VertexId<Tab>pagerankValue|neighbor,neighbor,neighbor .....
     */
    public void prepare(GraphDataSet dataset) {
        System.out.println("start page prepare");
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                writer.write(nextNode.getId() + "\t");
                writer.write(1 + "|");//init pagerank value
                Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                while (edgesIterator.hasNext()){
                    GraphNode end = edgesIterator.next().getEnd();
                    if (edgesIterator.hasNext()){
                        writer.write(end.getId() + ",");
                    }
                    else {
                        writer.write(end.getId() + "");
                    }
                    if(it.hasNext()){
                        writer.newLine();//换行
                    }
                }
            }
            System.out.println("finish page prepare");
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
        System.out.println("finish page pwd");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /input/" + "INPUT_NAME");
        System.out.println("finish page rm input");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -put " + INPUT_PATH + " /input");
        System.out.println("finish page put input");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/PageRankOutput");
        System.out.println("finish page rm output");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hadoop/hadoop-examples/hadoop-examples.jar cn.edu.tongji.PageRank.PageRank").toString();
        System.out.println("finish page jar");
        String lsString = Execute.exec("ls -l").toString();

        System.out.println("==========INFO=============");
        System.out.println(pwdString);
        System.out.println(lsString);
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.71", "192.168.1.72");
    }

    public static void main(String[] args) {
        //GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        //gpts.addTask("pagerank", new PageRank());
        //gpts.run();
        GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        PageRank toyTask = new PageRank();
        toyTask.prepare(d);
        toyTask.run();
    }
}
