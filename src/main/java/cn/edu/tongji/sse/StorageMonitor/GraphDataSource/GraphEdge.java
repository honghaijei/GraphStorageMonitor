package cn.edu.tongji.sse.StorageMonitor.GraphDataSource;

import java.util.Map;

/**
 * Created by hahong on 2016/7/29.
 */
public interface GraphEdge {
    Map<String, String> getAttributes();
    GraphNode getBegin();
    GraphNode getEnd();
}
