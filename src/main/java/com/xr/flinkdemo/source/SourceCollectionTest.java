package com.xr.flinkdemo.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceCollectionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 15553214155L, 15.4),
                new SensorReading("sensor_2", 155512355L, 18.4),
                new SensorReading("sensor_3", 15755555L, 5.1)
        ));
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 123);
        dataStreamSource.print("data");
        integerDataStreamSource.print("int");

        env.execute();
    }
}
