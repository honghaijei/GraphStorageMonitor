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
        //boolean isSource = true;
        System.out.println("start SSSP prepare");
        Iterator<GraphNode> it = dataset.iterator();
        File file = new File(INPUT_PATH);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            writer.write(27383 + "\t");//SourceNode Id
            writer.write("27384|0|GRAY|");
            while(it.hasNext()){
                GraphNode nextNode = it.next();
                if (nextNode.getId() != 27383){
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
                        writer.write("|Integer.MAX_VALUE|WHITE|");
                    }
                    if(it.hasNext()){
                        writer.newLine();//换行
                    }
                }
            }
            System.out.println("finish SSSP prepare");
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
        System.out.println("finish SSSP pwd");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /input/" + INPUT_NAME);
        System.out.println("finish SSSP rm input");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -put " + INPUT_PATH  + " /input");
        System.out.println("finish SSSP put input");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_1");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_2");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_3");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop fs -rm -r /output/output_graph_4");
        System.out.println("finish SSSP rm output");
        Execute.exec("/usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hadoop/hadoop-examples/hadoop-examples.jar cn.edu.tongji.SSSP.GraphSearch");
        System.out.println("finish SSSP jar");
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
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("sssp", new SSSP());
        gpts.run();
    }
}
