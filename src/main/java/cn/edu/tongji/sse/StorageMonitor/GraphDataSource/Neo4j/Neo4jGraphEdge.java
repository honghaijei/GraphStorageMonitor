package cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hahong on 2016/7/29.
 */
public class Neo4jGraphEdge implements GraphEdge {
    Neo4jGraphDataSet dataset;
    Neo4jGraphNode from;
    Neo4jGraphNode to;
    int id;
    public Neo4jGraphEdge(Neo4jGraphDataSet dataset, Neo4jGraphNode from, Neo4jGraphNode to, int id) {
        this.dataset = dataset;
        this.from = from;
        this.to = to;
        this.id = id;
    }
    @Override
    public Map<String, String> getAttributes() {
        String queryString = String.format("match ()-[n]->() where id(n)=%d return n", id);
        JSONObject obj = Neo4jUtils.query(dataset.getEndpoint(), dataset.getAuthorization(), queryString);
        Map<String, String> ans = new HashMap<>();
        try {
            obj = obj.getJSONArray("data").getJSONArray(0).getJSONObject(0).getJSONObject("data");
            Iterator it = obj.sortedKeys();
            while (it.hasNext()) {
                String key = (String)it.next();
                ans.put(key, obj.getString(key));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return ans;
    }

    @Override
    public GraphNode getBegin() {
        return this.from;
    }

    @Override
    public GraphNode getEnd() {
        return this.to;
    }
}
