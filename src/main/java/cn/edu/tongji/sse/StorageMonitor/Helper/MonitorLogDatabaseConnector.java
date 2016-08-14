package cn.edu.tongji.sse.StorageMonitor.Helper;

import cn.edu.tongji.sse.StorageMonitor.model.MySqlAllDataEntry;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hahong on 2016/8/3.
 */
public class MonitorLogDatabaseConnector {
    String drivename="com.mysql.jdbc.Driver";
    String url="jdbc:mysql://10.60.45.101/clusterdata";
    String user="root";
    String password="root";
    String insql;
    Connection conn;

    public Connection ConnectMysql(){
        try{
            Class.forName(drivename);
            conn = (Connection) DriverManager.getConnection(url, user, password);
            if (!conn.isClosed()) {
                System.out.println("Succeeded connecting to the Database!");
            } else {
                System.out.println("Falled connecting to the Database!");
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return conn;
    }

    public void CutConnection(Connection conn) throws SQLException {
        conn.close();
    }

    public boolean InsertSql(MySqlAllDataEntry e){
        try{
            insql="insert into alldata(jobid,machineid,starttime,endtime,jsoncontent) values(?,?,?,?,?)";
            PreparedStatement ps=conn.prepareStatement(insql);
            ps.setString(1, e.jobid);
            ps.setString(2, e.machineId);
            ps.setTimestamp(3, new Timestamp(e.startTime));
            ps.setTimestamp(4, new Timestamp(e.endTime));
            ps.setString(5, e.json);
            int result=ps.executeUpdate();
            if(result> 0)
                return true;
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return false;
    }
}
