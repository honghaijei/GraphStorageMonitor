package cn.edu.tongji.sse.StorageMonitor.GraphDataSource;

import java.util.Collection;
import java.util.Map;

/**
 * Created by hahong on 2016/7/29.
 */
public interface GraphNode {
    Map<String, String> getAttribute();
    Collection<GraphEdge> getOutEdges();
    Collection<GraphEdge> getInEdges();
    int getId();
}
