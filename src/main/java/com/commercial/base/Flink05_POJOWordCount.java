package com.commercial.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink05_POJOWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\code\\commercial\\input\\word.txt");
        SingleOutputStreamOperator<WordCount> sum = dataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> integerTuple2 = Tuple2.of(word, 1);
                    out.collect(new WordCount(integerTuple2.f0, integerTuple2.f1));
                }
            }
        }).keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount value) throws Exception {
                return value.getWord();
            }
        }).sum("count");//这块由于上面传的不是tuple，就只能用filed来表示要聚合得内容了
        sum.print();
        env.execute();


    }
}
