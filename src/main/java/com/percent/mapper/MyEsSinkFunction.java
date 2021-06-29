package com.percent.mapper;

import cn.hutool.core.util.StrUtil;
import com.percent.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;

/**
 * @author yunpeng.gu
 * @date 2021/1/20 21:32
 * @Email:yunpeng.gu@percent.cn
 */
public class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
    @Override
    public void process(SensorReading sensorReading,
                        RuntimeContext runtimeContext,
                        RequestIndexer requestIndexer
    ) {
        HashMap<String,String> dataSource = new HashMap<>();
        dataSource.put("id",sensorReading.getId());
        dataSource.put("ts", StrUtil.toString(sensorReading.getTime()));
        dataSource.put("temp",sensorReading.getTempd().toString());

        IndexRequest indexRequest = Requests.indexRequest()
                .index("sensor")
                .type("readingData")
                .source(dataSource);

        requestIndexer.add(indexRequest);
    }
}
