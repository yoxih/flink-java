package com.percent.mapper;

import cn.hutool.db.Db;
import com.percent.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 21:36
 * @Email:yunpeng.gu@percent.cn
 */
public class MyJdbcSink extends RichSinkFunction<SensorReading> {
    Connection conn = null;
    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = Db.use().getConnection();
        insertStmt = conn.prepareStatement(
                "insert into sensor_temp (id,tempd) values (?,?)"
        );
        updateStmt = conn.prepareStatement("" +
                "update sensor_temp set tempd = ? where id = ?"
        );
        super.open(parameters);
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        updateStmt.setDouble(1,value.getTempd());
        updateStmt.setString(2,value.getId());
        updateStmt.execute();
        if (updateStmt.getUpdateCount() == 0){
            insertStmt.setString(1, value.getId());
            insertStmt.setDouble(2, value.getTempd());
            insertStmt.execute();
        }
    }

    @Override
    public void close() throws Exception {
        insertStmt.close();
        updateStmt.close();
        conn.close();
        super.close();
    }
}
