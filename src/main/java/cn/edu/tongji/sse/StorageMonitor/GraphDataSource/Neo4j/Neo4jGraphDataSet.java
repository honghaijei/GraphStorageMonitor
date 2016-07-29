package cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j;

import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphDataSet;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphEdge;
import cn.edu.tongji.sse.StorageMonitor.GraphDataSource.GraphNode;
import org.json.JSONArray;
import org.json.JSONObject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Created by hahong on 2016/7/29.
 */
public class Neo4jGraphDataSet implements GraphDataSet<Neo4jGraphNode> {
    private List<Neo4jGraphNode> nodeIds = new ArrayList<>();
    private String endpoint;
    private String authorization;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAuthorization() {
        return authorization;
    }

    public void setAuthorization(String authorization) {
        this.authorization = authorization;
    }

    public Neo4jGraphDataSet(String neo4jEndpoint, String authorization) {
        this.endpoint = neo4jEndpoint;
        this.authorization = authorization;
        try {
            JSONObject obj = Neo4jUtils.query(neo4jEndpoint, authorization, "MATCH (it) return id(it)");
            JSONArray arr = obj.getJSONArray("data");
            for (int i = 0; i < arr.length(); i++) {
                int currentId = Integer.parseInt(arr.getJSONArray(i).getString(0));
                Neo4jGraphNode currentNode = new Neo4jGraphNode(this, currentId);
                nodeIds.add(currentNode);
            }
        }catch (Exception ex) {

        } finally {
        }

    }
    @Override
    public Iterator<Neo4jGraphNode> iterator() {
        return nodeIds.iterator();
    }

    @Override
    public Neo4jGraphNode get(Integer key) {
        throw new NotImplementedException();
    }

    public static void main(String[] args) {
        GraphDataSet d = new Neo4jGraphDataSet("http://10.60.45.79:7474", "Basic bmVvNGo6MTIzNDU2");
        Iterator<GraphNode> it = d.iterator();
        while (it.hasNext()) {
            GraphNode node = it.next();
            System.out.println(node.getId());
            List<GraphEdge> edges = new ArrayList<GraphEdge>(node.getInEdges());
            System.out.println(node.getInEdges().size());
            System.out.println(node.getOutEdges().size());
        }

    }
}
