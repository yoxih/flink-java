package com.percent.time;

import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author yunpeng.gu
 * @date 2021/1/26 17:25
 * @Email:yunpeng.gu@percent.cn
 */
public class FlinkTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用事件时间
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapStream = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTime() * 1000L;
            }
        });

//        mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10)) {
//            @Override
//            public long extractTimestamp(SensorReading element) {
//                return element.getTime()*1000;
//            }
//        });
        // 创建测输出流
        OutputTag<SensorReading> lateData = new OutputTag<SensorReading>("lateData"){};

        SingleOutputStreamOperator<SensorReading> minTempStream = mapStream.keyBy(SensorReading::getId)
                // 开启一个滚动窗口
                .timeWindow(Time.seconds(3))
                // 允许1分钟的迟到数据
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateData)
                .minBy("tempd");

        minTempStream.print();
        minTempStream.getSideOutput(lateData).print();

        env.execute();
    }
}
