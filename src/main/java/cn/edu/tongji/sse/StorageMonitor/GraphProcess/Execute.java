package cn.edu.tongji.sse.StorageMonitor.GraphProcess;

import java.io.InputStreamReader;
import java.io.LineNumberReader;

/**
 * Created by chenran on 2016/8/2 0002.
 */

/*
Execute Linux command
 */
public class Execute {
    public static Object exec(String cmd) {
        try {
            String[] cmdA = { "/bin/sh", "-c", cmd };
            Process process = Runtime.getRuntime().exec(cmdA);
            LineNumberReader br = new LineNumberReader(new InputStreamReader(
                    process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                sb.append(line).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
