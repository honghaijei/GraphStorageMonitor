package cn.edu.tongji.sse.StorageMonitor.GraphDataSource;

import java.util.Iterator;

/**
 * Created by hahong on 2016/7/29.
 */
public interface GraphDataSet<T extends GraphNode> {
    Iterator<T> iterator();
    T get(Integer id);
}
