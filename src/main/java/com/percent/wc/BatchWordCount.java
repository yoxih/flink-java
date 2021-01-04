package com.percent.wc;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * @author yunpeng.gu
 * @date 2020/12/6 18:15
 * @Email:yunpeng.gu@percent.cn
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "D:\\IdeaProjects\\flink-java\\src\\main\\resources\\wc.txt";
        DataSet<String> dataSet = env.readTextFile(filePath);
        DataSet<Tuple2<String, Integer>> sum = dataSet.flatMap(new MyFlatMapper())
                .groupBy(0).sum(1);
        sum.print();
    }

     static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

         @Override
         public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
             String[] words = value.split(" ");
             for (String word : words) {
                 out.collect(new Tuple2<>(word,1));
             }
         }
     }
}
