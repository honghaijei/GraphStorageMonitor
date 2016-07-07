package cn.edu.tongji.sse.StorageMonitor.Kafka;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class PageRankSingle {
    DirectedGraph<Integer, String> g =
            new DirectedSparseGraph<Integer, String>();

    public void readFile(String[] filename) throws IOException {

        for (int i = 0; i < filename.length; i++) {
            String[] result = filename[i].split("-");
            g.addEdge(result[0] + " " + result[1],
                    Integer.parseInt(result[0]),
                    Integer.parseInt(result[1]));
        }
    }
}
