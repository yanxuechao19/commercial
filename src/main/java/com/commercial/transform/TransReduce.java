package com.commercial.transform;

import com.commercial.base.ClickSource;
import com.commercial.base.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// todo reduce方法--》value1 – The first value to combine. value2 – The second value to combine. 第一个参数是已经之前聚合完的结果，第二个参数是当前新传入的值，然后在reduce方法中进行二者的聚合（比较）逻辑
public class TransReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = env.addSource(new ClickSource()).map(new MapFunction<Event, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1);
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                }).keyBy(r -> true)//todo 将前面的分组 都放进一条流  用于比较大小
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return stringIntegerTuple2.f1 > t1.f1 ? stringIntegerTuple2 : t1;
                    }
                });
        reduce.print();
        env.execute();

    }
}
