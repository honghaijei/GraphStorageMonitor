package cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by hahong on 2016/7/29.
 */
public class Neo4jGraphNode implements GraphNode {
    private int id;
    private Neo4jGraphDataSet dataset;

    public Neo4jGraphNode(Neo4jGraphDataSet dataset, int id) {
        this.id = id;
        this.dataset = dataset;
    }
    @Override
    public Map<String, String> getAttribute() {
        String queryString = String.format("MATCH (it) where id(it)=%d return it", id);
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
    public Collection<GraphEdge> getOutEdges() {
        String queryString = String.format("match (a-[c]->b) where id(a)=%d return id(a),id(c),id(b)", id);
        JSONObject obj = Neo4jUtils.query(dataset.getEndpoint(), dataset.getAuthorization(), queryString);
        List<GraphEdge> edges = new ArrayList<>();
        try {
            JSONArray arr = obj.getJSONArray("data");
            for (int i = 0; i < arr.length(); ++i) {
                int from = arr.getJSONArray(i).getInt(0);
                int to = arr.getJSONArray(i).getInt(2);
                int id = arr.getJSONArray(i).getInt(1);
                edges.add(new Neo4jGraphEdge(this.dataset, new Neo4jGraphNode(this.dataset, from), new Neo4jGraphNode(this.dataset, to), id));
            }
            return edges;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Collection<GraphEdge> getInEdges() {
        String queryString = String.format("match (a<-[c]-b) where id(a)=%d return id(a),id(c),id(b)", id);
        JSONObject obj = Neo4jUtils.query(dataset.getEndpoint(), dataset.getAuthorization(), queryString);
        List<GraphEdge> edges = new ArrayList<>();
        try {
            JSONArray arr = obj.getJSONArray("data");
            for (int i = 0; i < arr.length(); ++i) {
                int from = arr.getJSONArray(i).getInt(0);
                int to = arr.getJSONArray(i).getInt(2);
                int id = arr.getJSONArray(i).getInt(1);
                edges.add(new Neo4jGraphEdge(this.dataset, new Neo4jGraphNode(this.dataset, from), new Neo4jGraphNode(this.dataset, to), id));
            }
            return edges;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getId() {
        return this.id;
    }
}
