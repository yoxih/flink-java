package com.percent.sink;

import com.percent.beans.SensorReading;
import com.percent.mapper.MyEsSinkFunction;
import com.percent.mapper.MyJdbcSink;
import com.percent.mapper.MyRedisMapper;
import com.percent.source.MySensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.http.HttpHost;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 11:58
 * @Email:yunpeng.gu@percent.cn
 */
public class Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorDataStreamSource = env.addSource(new MySensorReading());

        // kafka
        FlinkKafkaProducer011<String> kafkaSink2 = new FlinkKafkaProducer011<>(
                "172.24.5.251:9092",
                "sensor",
                new SimpleStringSchema()
        );
//        sensorDataStreamSource.addSink(kafkaSink2);

        // redis
        HashSet<InetSocketAddress> set = new HashSet<>(1);
        set.add(InetSocketAddress.createUnresolved("172.24.5.251",7000));
        FlinkJedisClusterConfig redisConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(set)
                .build();
        MyRedisMapper myRedisMapper = new MyRedisMapper();
//        sensorDataStreamSource.addSink(new RedisSink<>(redisConfig, myRedisMapper));

        // Elasticsearch
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.24.5.251",7000));
        ElasticsearchSink<SensorReading> esSink = new ElasticsearchSink.Builder<SensorReading>(
                httpHosts, new MyEsSinkFunction()
        ).build();
//        sensorDataStreamSource.addSink(esSink);

        // mysql
        sensorDataStreamSource.addSink(new MyJdbcSink());
        sensorDataStreamSource.print();
        env.execute("flink_sink_to_kafka");

    }
}
