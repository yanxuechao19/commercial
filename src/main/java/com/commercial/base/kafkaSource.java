package com.commercial.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class kafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka.data-analysis.svc.cluster.local:9092")
                .setTopics("growalong_prod")
                .setGroupId("growalong_prod_20240620")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //.setProperty("isolation.level","read_committed") //当不知道如何设置时候 可以用这个方法 以kv形式传参 该方法可连续调用多次
                .build();

        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        ds.print();
        env.execute();

    }
    //todo


}
