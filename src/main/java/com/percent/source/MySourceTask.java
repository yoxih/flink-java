package com.percent.source;

import com.percent.beans.Sensor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yunpeng.gu
 * @date 2021/1/13 23:31
 * @Email:yunpeng.gu@percent.cn
 */
public class MySourceTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> dataStream = env.addSource(new MySensor());
        dataStream.print();
        env.execute();
    }
}
