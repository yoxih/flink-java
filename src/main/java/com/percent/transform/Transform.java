package com.percent.transform;

import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import sun.awt.windows.WPrinterJob;

import javax.swing.*;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author yunpeng.gu
 * @date 2021/1/14 22:26
 * @Email:yunpeng.gu@percent.cn
 */
public class Transform {
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

        // map
//        DataStream<String> mapStream = sensorDataStreamSource.map((MapFunction<Sensor, String>) value -> value.getTempd());

//        mapStream.print();

        // flatMap
//        DataStream<String> flatMpaStream = sensorDataStreamSource.flatMap(new FlatMapFunction<Sensor, String>() {
//            @Override
//            public void flatMap(Sensor value, Collector<String> out) throws Exception {
//                String[] split = value.getName().split("_");
//                for (String s : split) {
//                    out.collect(s);
//                }
//            }
//        });
//        flatMpaStream.print();

        // Filter
//        SingleOutputStreamOperator<Sensor> filterStream = sensorDataStreamSource.filter(new FilterFunction<Sensor>() {
//            @Override
//            public boolean filter(Sensor value) throws Exception {
//                Double tempd = Double.valueOf(value.getTempd());
//                return tempd > 50 ? true : false;
//            }
//        });
//        filterStream.print();

        // KeyBy
//        KeyedStream<Sensor, Tuple> sensorKeyStream = sensorDataStreamSource.keyBy("name");
        // 当传入指定的类属性时，可以返回该属性值类型的数据
//        KeyedStream<Sensor, String> sensorStringKeyedStream = sensorDataStreamSource.keyBy(Sensor::getName);
//        SingleOutputStreamOperator<Sensor> resultStream = sensorKeyStream.min("time");
//        resultStream.print();
//        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStreamSource.keyBy("id");
//        SingleOutputStreamOperator<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                SensorReading value = value1.getTempd() < value2.getTempd() ? value1 : value2;
//                return value;
//            }
//        });
//        reduceStream.print();

        // Split和Select
        SplitStream<SensorReading> splitStream = sensorDataStreamSource.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTempd() > 30
                        ? Collections.singletonList("high")
                        : Collections.singletonList("low"));
            }
        });
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> AllStream = splitStream.select("high", "low");

//        // 合流Connect
//        SingleOutputStreamOperator<Tuple2<String, Double>> warnStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(SensorReading value) throws Exception {
//                return new Tuple2<>(value.getId(), value.getTempd());
//            }
//        });
//        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warnStream.connect(lowStream);
//
//        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
//            @Override
//            public Object map1(Tuple2<String, Double> value) throws Exception {
//                return new Tuple3<>(value.f0, value.f1, "warning");
//            }
//
//            @Override
//            public Object map2(SensorReading value) throws Exception {
//                return new Tuple2<>(value.getId(), "healthy");
//            }
//        });
//        resultStream.print();

        DataStream<SensorReading> unionStreams = highStream.union(lowStream);
        unionStreams.print();
        AllStream.print();
        env.execute();

    }
}
