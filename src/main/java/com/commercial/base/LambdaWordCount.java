package com.commercial.base;

import org.apache.flink.api.common.typeinfo.TypeHint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class LambdaWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\code\\commercial\\input\\word.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> rs = dataStreamSource.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> integerTuple2 = Tuple2.of(word, 1);
                collector.collect(integerTuple2);
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        }).keyBy(value -> value.f0).sum(1);
        rs.print();
        env.execute();
    }
}
