package com.percent.source;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 15:17
 * @Email:yunpeng.gu@percent.cn
 */
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.percent.beans.Sensor;
import com.percent.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author yunpeng.gu
 * @date 2021/1/13 23:34
 * @Email:yunpeng.gu@percent.cn
 */
public class MyJsonSensor implements SourceFunction<String> {

    private boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>(); for(int i = 0; i < 10; i++ ){
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (running) {
            for (String sensorId : sensorTempMap.keySet()) {
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian(); sensorTempMap.put(sensorId, newTemp);
                SensorReading sensorReading = new SensorReading(sensorId, System.currentTimeMillis(), newTemp);
                ctx.collect(JSONUtil.toJsonStr(sensorReading));
            }
            ThreadUtil.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}