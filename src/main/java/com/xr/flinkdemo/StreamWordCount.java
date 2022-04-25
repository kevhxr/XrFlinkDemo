package com.xr.flinkdemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(4);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("host");
        Integer portNum = parameterTool.getInt("port");

        //read from stream
        DataStreamSource<String> inputDataStream = env.socketTextStream(hostName, portNum);

        //based on data stream do convert calculation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream =
                inputDataStream.flatMap(new WordCountTest.MyFlatMapper()).slotSharingGroup("green")
                .keyBy(0)
                .sum(1).slotSharingGroup("red");
        //print
        resultStream.print().setParallelism(1);
        //resultStream.print().setParallelism(1);

        //execute task
        env.execute();
    }
}
