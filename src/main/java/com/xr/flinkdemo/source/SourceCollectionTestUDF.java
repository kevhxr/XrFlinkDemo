package com.xr.flinkdemo.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

public class SourceCollectionTestUDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<SensorReading> dataStreamSource = env.addSource(new MySensorSource());


        dataStreamSource.print("sensorData");
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        private Boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            Random random = new Random();

            HashMap<String, Double> sensorTempMap = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId)+random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
