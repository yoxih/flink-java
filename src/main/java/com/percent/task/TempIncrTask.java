package com.percent.task;

import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * @author yunpeng.gu
 * @date 2021/1/27 22:22
 * @Email:yunpeng.gu@percent.cn
 */
public class TempIncrTask {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sensorDataStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<SensorReading> sensorMapStream = sensorDataStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });
        sensorMapStream.
                keyBy(SensorReading::getId)
                .flatMap(new MyFlatMapFunction(10.0))
                .print();
        env.execute();
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<SensorReading,Tuple3<String,Double,Double>>{

        private Double threshold;
        private ValueState<Double> valueState;

        public MyFlatMapFunction(Double threshold){
            this.threshold = threshold;
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTempd = this.valueState.value();
            if (this.valueState.value() != null){
                double diff = Math.abs(value.getTempd() - lastTempd);
                if (diff > threshold){
                    out.collect(new Tuple3<>(value.getId(),lastTempd,value.getTempd()));
                }
            }
            this.valueState.update(value.getTempd());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("key-valueState",Double.class,0.0));
        }

        @Override
        public void close() throws Exception {
            this.valueState.clear();
        }
    }
}
