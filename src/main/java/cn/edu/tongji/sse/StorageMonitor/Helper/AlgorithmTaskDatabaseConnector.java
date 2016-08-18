package cn.edu.tongji.sse.StorageMonitor.Helper;

import cn.edu.tongji.sse.StorageMonitor.model.MySqlAllDataEntry;
import cn.edu.tongji.sse.StorageMonitor.model.MySqlTaskEntry;

import java.sql.*;

/**
 * Created by hahong on 2016/8/18.
 */
public class AlgorithmTaskDatabaseConnector {
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

    public boolean InsertSql(MySqlTaskEntry e){
        try{
            insql="insert into tasks(taskid,taskdescription,tasktype,createtime,status) values(?,?,?,?,?)";
            PreparedStatement ps=conn.prepareStatement(insql);
            ps.setString(1, e.taskid);
            ps.setString(2, e.taskDescription);
            ps.setString(3, e.taskType);
            ps.setTimestamp(4, new Timestamp(e.createtime));
            ps.setString(5, e.status);
            int result=ps.executeUpdate();
            if(result> 0)
                return true;
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return false;
    }
}
