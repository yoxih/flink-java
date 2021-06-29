package com.percent.function;

import com.percent.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yunpeng.gu
 * @date 2021/1/15 10:42
 * @Email:yunpeng.gu@percent.cn
 */
public class UdfFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        // 匿名类
//        SingleOutputStreamOperator<SensorReading> filterStream = sensorDataStreamSource.filter(new FilterFunction<SensorReading>() {
//            @Override
//            public boolean filter(SensorReading value) throws Exception {
//                return value.getId().contains("_1");
//            }
//        });
//        filterStream.print();

        // 静态内部类
//        SingleOutputStreamOperator<SensorReading> filterStream = sensorDataStreamSource.filter(new KeyWordFilterFunction("_2"));
//        filterStream.print();

        // 匿名函数
//        SingleOutputStreamOperator<SensorReading> filterStream = sensorDataStreamSource.filter(sensor -> sensor.getTempd() > 30);
//        filterStream.print();
        SingleOutputStreamOperator<Tuple2<String, String>> mapStream = sensorDataStreamSource.map(new MyMapFunction());
        mapStream.print();
        env.execute();

    }

    private static class KeyWordFilterFunction implements FilterFunction<SensorReading>{
        private String keyWord ;

        public KeyWordFilterFunction(String keyWord){
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.getId().contains(keyWord);
        }
    }

    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple2<String,String>>{

        @Override
        public Tuple2<String, String> map(SensorReading value) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            return new Tuple2<String, String>(value.getId(),getRuntimeContext().getTaskName());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("在调用时先调用open");
            System.out.println(parameters);
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            System.out.println("最后调用close关闭");
            super.close();
        }
    }
}
