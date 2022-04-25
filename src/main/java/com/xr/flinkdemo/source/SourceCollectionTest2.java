package com.xr.flinkdemo.source;

import com.xr.flinkdemo.WordCountTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceCollectionTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        String inputPath = WordCountTest.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStream<String> dataStreamSource = env.readTextFile(inputPath);
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 123);
        dataStreamSource.print("data");
        integerDataStreamSource.print("int");

        env.execute();
    }
}
