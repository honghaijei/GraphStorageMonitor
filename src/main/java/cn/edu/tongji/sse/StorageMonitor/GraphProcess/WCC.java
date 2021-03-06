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
 * Created by chenran on 2016/8/2 0002.
 */
public class WCC implements AlgorithmTask{

    /**input file name*/
    public static final String INPUT_NAME = "WCCInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/WCCInput";
    @Override
    /*
    data format:
    VertexId<Tab>neighbor<Tab>neighbor....
     */
    public void prepare(GraphDataSet dataset) {
        System.out.println("start wcc prepare");
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                writer.write(nextNode.getId() + "");//Vertex Id
                Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                while (edgesIterator.hasNext()){
                    //neighbors
                    GraphNode end = edgesIterator.next().getEnd();
                    writer.write("\t" + end.getId());
                }
                if(it.hasNext()){
                    writer.newLine();//换行
                }
            }
            System.out.println("finish wcc prepare");
            writer.flush();
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /input/" + INPUT_NAME);
            System.out.println("finish wcc rm input");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -put " + INPUT_PATH  + " /input");
            System.out.println("finish wcc put input");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/DMSTOutput1");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/DMSTOutput2");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/DMSTOutputfinal");
            System.out.println("finish wcc rm output");
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

        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hadoop/hadoop-examples/hadoop-examples.jar cn.edu.tongji.CCC.MindistSearchJob");
        System.out.println("finish wcc jar");
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.61", "192.168.1.62");
    }

    /*
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("wcc", new WCC());
        gpts.run();
        //GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        //WCC toyTask = new WCC();
        //toyTask.prepare(d);
        //toyTask.run();
    }
    */
}
