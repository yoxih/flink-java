package com.percent.task;

import com.percent.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author yunpeng.gu
 * @date 2021/1/21 21:31
 * @Email:yunpeng.gu@percent.cn
 */
public class WindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<SensorReading> mapStream = dataStreamSource.map(line -> {
            try{
                String[] split = line.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }catch (Exception e){
                System.out.println("数据解析失败"+line);
                return null;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTime() * 1000L;
            }
        });

//        mapStream.keyBy(SensorReading::getId)
//                .timeWindow(Time.seconds(15))
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                        return 0;
//                    }
//
//                    @Override
//                    public Integer add(SensorReading value, Integer accumulator) {
//                        return accumulator + 1;
//                    }
//
//                    @Override
//                    public Integer getResult(Integer accumulator) {
//                        return accumulator;
//                    }
//
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        return a + b;
//                    }
//                }).print();
//        mapStream.keyBy(SensorReading::getId)
//                .timeWindow(Time.seconds(15),Time.seconds(5))
//                .minBy("tempd")
//                .print();
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        mapStream.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(3))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(outputTag)
                .minBy("tempd")
                .print("mapStream");
        mapStream.getSideOutput(outputTag).print();
        env.execute();
    }
}
