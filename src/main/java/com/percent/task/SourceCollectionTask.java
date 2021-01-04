package com.percent.task;

import com.percent.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yunpeng.gu
 * @date 2021/1/4 22:49
 * @Email:yunpeng.gu@percent.cn
 */
public class SourceCollectionTask {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Sensor> sensorDataStreamSource = env.fromCollection(
                Arrays.asList(
                        new Sensor("sensor_1", System.currentTimeMillis(), "56.8"),
                        new Sensor("sensor_2", System.currentTimeMillis(), "76.8"),
                        new Sensor("sensor_3", System.currentTimeMillis(), "86.8"),
                        new Sensor("sensor_4", System.currentTimeMillis(), "26.8"),
                        new Sensor("sensor_5", System.currentTimeMillis(), "46.8")
                )
        );
        sensorDataStreamSource.map(
                (MapFunction<Sensor, Sensor>) value -> {
                    value.setTempd(value.getTempd()+20);
                    return value;
                }
        );
        sensorDataStreamSource.print();
        env.execute("sensor_collection_source");
    }
}
