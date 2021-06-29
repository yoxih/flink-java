package com.percent.transform;

import com.percent.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yunpeng.gu
 * @date 2021/1/19 20:43
 * @Email:yunpeng.gu@percent.cn
 */
public class Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<SensorReading> sensorDataStreamSource = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1610636277388L, 56.8),
                        new SensorReading("sensor_2", 1610636277381L, 76.8),
                        new SensorReading("sensor_3", 1610636277382L, 86.8),
                        new SensorReading("sensor_4", 1610636277383L, 26.8),
                        new SensorReading("sensor_5", 1610636277384L, 46.8),
                        new SensorReading("sensor_1", 1610636277385L, 26.8),
                        new SensorReading("sensor_2", 1610636277386L, 16.8),
                        new SensorReading("sensor_3", 1610636277387L, 11.8)
                )
        );
        sensorDataStreamSource.print("input");

        // 1. shuffle :类似于发牌，来一个数据将该数据重新分配一个分区发送下去
        DataStream<SensorReading> shuffleDataStream = sensorDataStreamSource.shuffle();
        shuffleDataStream.print("shuffle");
        // 2. keyBy :根据指定key进行hash分区
        KeyedStream<SensorReading, String> keyedStream = sensorDataStreamSource.keyBy(SensorReading::getId);
        keyedStream.print("keyed");
        // 3. global :将数据全部放到同一个分区
        DataStream<SensorReading> globalStream = sensorDataStreamSource.global();
        globalStream.print("global");

        env.execute();
    }
}
