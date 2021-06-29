package com.percent.source;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.percent.beans.Sensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author yunpeng.gu
 * @date 2021/1/13 23:34
 * @Email:yunpeng.gu@percent.cn
 */
public class MySensor implements SourceFunction<Sensor> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Sensor> ctx) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>(); for(int i = 0; i < 10; i++ ){
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (running) {
            for (String sensorId : sensorTempMap.keySet()) {
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian(); sensorTempMap.put(sensorId, newTemp);
                ctx.collect( new Sensor(sensorId, System.currentTimeMillis(), StrUtil.toString(newTemp)));
            }
            ThreadUtil.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
