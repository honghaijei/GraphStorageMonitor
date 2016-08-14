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
public class DMST implements AlgorithmTask {
    /**input file name*/
    public static final String INPUT_NAME = "DMSTInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/DMSTInput";
    @Override
    /*
    data format:
    edgeValue<Tab>neighbor<Tab>neighbor....
     */
    public void prepare(GraphDataSet dataset) {
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            System.out.println("start DMST prepare");
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                if (nextNode.getOutEdges().size() != 0){
                    Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                    Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                    while (edgesIterator.hasNext()){
                        writer.write(1 + "");//edge value
                        //neighbors
                        GraphEdge nextEdge = edgesIterator.next();
                        GraphNode begin = nextEdge.getBegin();
                        GraphNode end = nextEdge.getEnd();
                        writer.write("\t" + begin.getId());
                        writer.write("\t" + end.getId());
                        if(it.hasNext() || edgesIterator.hasNext()){
                            writer.newLine();//换行
                        }
                    }
                }
            }
            System.out.println("finish DMST prepare");
            writer.flush();
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /input/" + INPUT_NAME);
            System.out.println("finish DMST rm input");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -put " + INPUT_PATH  + " /input");
            System.out.println("finish DMST put input");
            Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/DMSTOutput");
            System.out.println("finish dmst rm output");
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
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hadoop/hadoop-examples/hadoop-examples.jar cn.edu.tongji.DMST.DMST");
        System.out.println("finish dmst jar");
    }

    @Override
    public Collection<String> getMachines() {
        return Arrays.asList("192.168.1.71", "192.168.1.72");
    }

    /*
    public static void main(String[] args) {
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("dmst", new DMST());
        gpts.run();
       //GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        //DMST toyTask = new DMST();
        //toyTask.prepare(d);
        //toyTask.run();
    }
    */
}
