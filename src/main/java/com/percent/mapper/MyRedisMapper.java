package com.percent.mapper;

import com.percent.beans.SensorReading;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 20:57
 * @Email:yunpeng.gu@percent.cn
 */
public class MyRedisMapper implements RedisMapper<SensorReading> {

    // 保存为HASHSET
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(
                RedisCommand.HSET,
                "sensor_tempe"
                );
    }

    @Override
    public String getKeyFromData(SensorReading sensorReading) {
        return sensorReading.getId();
    }

    @Override
    public String getValueFromData(SensorReading sensorReading) {
        return sensorReading.getTempd().toString();
    }
}
