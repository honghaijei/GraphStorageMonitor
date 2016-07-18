package cn.edu.tongji.sse.StorageMonitor.Monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by honghaijie on 7/11/16.
 */
public class MonitorUtils {
    public static class NetworkIO {
        long send;
        long receive;
        public NetworkIO() {}

        public NetworkIO(long send, long receive) {
            this.send = send;
            this.receive = receive;
        }

        public long getSend() {
            return send;
        }

        public void setSend(long send) {
            this.send = send;
        }

        public long getReceive() {
            return receive;
        }

        public void setReceive(long receive) {
            this.receive = receive;
        }


    }
    public static String executeCommands(String path) throws IOException {

        File tempScript = new File(path);
        StringBuilder sb = new StringBuilder();
        try {
            ProcessBuilder pb = new ProcessBuilder("bash", tempScript.toString());
            Process process = pb.start();
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sb.append(inputLine + "\n");
            }
            process.waitFor();
            in.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
        return sb.toString();
    }
    public static long getMemoryUsage() throws IOException {
        String memoryRawStr = executeCommands("scripts/memory.sh");
        String[] splitedMemotyStr = memoryRawStr.split("\n");
        //double totalMemory = Double.parseDouble(splitedMemotyStr[0]);
        long usedMemory = Long.parseLong(splitedMemotyStr[1]);
        //double memoryUsage = usedMemory / totalMemory;
        return usedMemory;
    }
    public static NetworkIO getNetWorkUsage() throws IOException {
        String networkRawStr = executeCommands("scripts/network.sh");
        String[] splitedNetworkStr = networkRawStr.split("\n");
        long receive = 0L, send = 0L;
        for (int i = 2; i < splitedNetworkStr.length; ++i) {
            if (splitedNetworkStr[i].contains("lo")) continue;
            String[] attr = splitedNetworkStr[i].trim().split("\\s+");
            receive += Long.parseLong(attr[1]);
            send += Long.parseLong(attr[9]);
        }
        NetworkIO ans = new NetworkIO(send, receive);
        return ans;
    }
    public static double getCPUUsage() throws IOException {
        String cpuRawStr = executeCommands("scripts/cpu.sh");
        return Double.parseDouble(cpuRawStr) / 100.0;
    }
}
