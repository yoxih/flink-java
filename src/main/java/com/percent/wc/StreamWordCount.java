package com.percent.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yunpeng.gu
 * @date 2020/12/6 18:24
 * @Email:yunpeng.gu@percent.cn
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);
        String host = "127.0.0.1";
        Integer port = 10011;
        if (param.get("host") != null){
            host = param.get("host");
        }
        if (param.get("port") != null){
            port = param.getInt("port");
        }
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new BatchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute();
    }
}
