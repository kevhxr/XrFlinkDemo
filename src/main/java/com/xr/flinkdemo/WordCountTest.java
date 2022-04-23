package com.xr.flinkdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //read data from file
        String inputPath = WordCountTest.class.getClassLoader().getResource("hi.txt").getPath();
        System.out.println(inputPath);
        DataSource<String> inputDataSet = env.readTextFile(inputPath);
        AggregateOperator<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper() {
                })
                .groupBy(0)
                .sum(1);
        resultSet.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
