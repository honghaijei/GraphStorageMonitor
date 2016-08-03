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
public class SSSP implements AlgorithmTask{

    /**input file name*/
    public static final String INPUT_NAME = "SSSPInput";
    /**input data path*/
    public static final String INPUT_PATH = "/tmp/SSSPInput";
    /*
    data format:
    SourceId<Tab>neighbor,neighbor|0|GRAY|
    VertexId<Tab>neighbor,neighbor|Integer.MAX_VALUE|WHITE|
     */
    @Override
    public void prepare(GraphDataSet dataset) {
        boolean isSource = true;
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            while(it.hasNext()){
                if(isSource == true){
                    GraphNode nextNode = it.next();
                    writer.write(nextNode.getId() + "\t");//SourceNode Id
                    Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                    Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                    while (edgesIterator.hasNext()){
                        //Neighbors
                        GraphNode end = edgesIterator.next().getEnd();
                        writer.write(end.getId() + "");
                        if(edgesIterator.hasNext()){
                            writer.write(",");
                        }
                    }
                    writer.write("|0|GRAY|");
                    if(it.hasNext()){
                        writer.newLine();//换行
                    }
                    isSource = false;
                }
                else{
                    GraphNode nextNode = it.next();
                    writer.write(nextNode.getId() + "\t");//Vertex Id
                    Collection<GraphEdge> outEdges = nextNode.getOutEdges();
                    Iterator<GraphEdge> edgesIterator = outEdges.iterator();
                    while (edgesIterator.hasNext()){
                        //Neighbors
                        GraphNode end = edgesIterator.next().getEnd();
                        writer.write(end.getId() + "");
                        if(edgesIterator.hasNext()){
                            writer.write(",");
                        }
                    }
                    writer.write("|Integer.MAX_VALUE|WHITE|");
                    if(it.hasNext()){
                        writer.newLine();//换行
                    }
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
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /input/" + INPUT_NAME);
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -put " + INPUT_PATH  + " /input");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_1");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_2");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_3");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_4");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar examples/hadoop-examples.jar cn.edu.tongji.SSSP.GraphSearch");
        String lsString = Execute.exec("ls -l").toString();

        System.out.println("==========INFO=============");
        System.out.println(pwdString);
        System.out.println(lsString);
    }
    public static void main(String[] args) {
        //GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        //gpts.setTask("sssp", new SSSP());
        //gpts.run();
        GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        SSSP ssspTask = new SSSP();
        ssspTask.prepare(d);
        ssspTask.run();
    }
}
