package com.percent.source;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 21:11
 * @Email:yunpeng.gu@percent.cn
 */
public class MySensorReading implements SourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceFunction.SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>(); for(int i = 0; i < 10; i++ ){
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        for (int i = 0; i < 16; i++) {
            for (String sensorId : sensorTempMap.keySet()) {
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian(); sensorTempMap.put(sensorId, newTemp);
                ctx.collect( new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
            }
//            ThreadUtil.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
