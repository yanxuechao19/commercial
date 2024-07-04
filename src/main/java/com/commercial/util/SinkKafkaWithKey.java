package com.commercial.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * packageName com.commercial.util
 *
 * @author yanxuechao
 * @version JDK 8
 * @className SinkKafkaWithKey
 * @date 2024/7/4
 * @description TODO
 */
public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());


        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("kafka.bootstrap.servers=b-1.prodmskcluster.zmqk4z.c5.kafka.us-east-1.amazonaws.com:9092,b-2.prodmskcluster.zmqk4z.c5.kafka.us-east-1.amazonaws.com:9092", 7777);


        /**
         * 如果要指定写入kafka的key，可以自定义序列化器：
         * 1、实现 一个接口，重写 序列化 方法
         * 2、指定key，转成 字节数组
         * 3、指定value，转成 字节数组
         * 4、返回一个 ProducerRecord对象，把key、value放进去
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka.bootstrap.servers=b-1.prodmskcluster.zmqk4z.c5.kafka.us-east-1.amazonaws.com:9092,b-2.prodmskcluster.zmqk4z.c5.kafka.us-east-1.amazonaws.com:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {

                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws", key, value);
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("frog")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();


        sensorDS.sinkTo(kafkaSink);


        env.execute();
    }
}

