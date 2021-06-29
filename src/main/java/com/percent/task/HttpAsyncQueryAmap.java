package com.percent.task;

import com.percent.beans.OrderBean;
import com.percent.function.AsyncHttpQueryFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author yunpeng.gu
 * @date 2021/6/18 11:17
 * @Email:yunpeng.gu@percent.cn
 */
public class HttpAsyncQueryAmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\amap.txt", "utf-8");
        SingleOutputStreamOperator<OrderBean> result = AsyncDataStream.unorderedWait(
                lines,
                new AsyncHttpQueryFunction(),
                2000,
                TimeUnit.MILLISECONDS,
                10
        );
        result.print();
        env.execute("HttpAsyncQueryAmap");
    }
}
