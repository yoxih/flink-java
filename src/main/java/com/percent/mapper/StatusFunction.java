package com.percent.mapper;

import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yunpeng.gu
 * @date 2021/1/27 21:59
 * @Email:yunpeng.gu@percent.cn
 */
public class StatusFunction {
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
        sensorDataStreamSource.keyBy(SensorReading::getId)
                .map(new MyStatusMapper())
                .print();
        env.execute();

    }

    public static class MyStatusMapper extends RichMapFunction<SensorReading,Integer>{
        private ValueState<Integer> keyValueState;

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = keyValueState.value();
            count++;
            keyValueState.update(count);
            System.out.println(count);
            return count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.keyValueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key_count", Integer.class, 0));
            super.open(parameters);
        }
    }
}
