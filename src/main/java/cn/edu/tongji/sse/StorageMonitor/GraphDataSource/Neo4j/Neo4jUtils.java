package cn.edu.tongji.sse.StorageMonitor.GraphDataSource.Neo4j;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Created by hahong on 2016/7/29.
 */
public class Neo4jUtils {
    public static JSONObject query(String endPoint, String authorization, String q) {
        try {
            DefaultHttpClient httpclient = new DefaultHttpClient();
            HttpPost request = new HttpPost(endPoint + "/db/data/cypher");
            JSONObject json = new JSONObject();
            json.put("query", q);
            StringEntity params = new StringEntity(json.toString());
            request.addHeader("Content-Type", "application/json");
            request.addHeader("Authorization", authorization);
            request.addHeader("Accept", "application/json; charset=UTF-8");
            request.setEntity(params);
            HttpResponse response = httpclient.execute(request);

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = rd.readLine()) != null) {
                sb.append(line);
            }
            JSONObject obj = new JSONObject(sb.toString());
            return obj;
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
