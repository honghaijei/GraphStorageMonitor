package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;

import java.io.*;
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
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -rmr /input/" + INPUT_NAME);
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -put "+ INPUT_PATH  + " /input");
        Execute.exec("/usr/local/hadoop/bin/hadoop fs -rmr /output/WCCOutput");
        Execute.exec("/usr/local/hadoop/./bin/hadoop jar " +
                "$GIRAPH_HOME/giraph-examples/target/giraph-examples-1.2.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar " +
                "org.apache.giraph.GiraphRunner org.apache.giraph.examples.ConnectedComponentsComputation " +
                "-vif org.apache.giraph.io.formats.IntIntNullTextInputFormat " +
                "-vip /input/WCCInput -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat " +
                "-op /output/WCCOutput " +
                "-w 1 ");
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
        GraphProcessTaskScheduler gpts = new GraphProcessTaskScheduler();
        gpts.addTask("wcc-giraph", new WCCGiraph());
        gpts.run();
    }
}
