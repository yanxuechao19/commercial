package com.commercial.Richfunction;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * packageName com.commercial.Richfunction
 *
 * @author yanxuechao
 * @version JDK 8
 * @className ConnectKeybyDemo (此处以class为例)
 * @date 2024/7/2
 * @description TODO
 */
public class ConnectKeybyDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        // todo 多并行度下，需要根据 关联条件 进行keyby，才能保证key相同的数据到一起去，才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> concectkey = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        
/*        concectkey.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            //todo 实现两条流互相join的效果 由于哪条流数据先来是不确定的，因此引入一个用来临时存储数据的媒介，当一条流数据到了，先放入媒介，然后去另外一条流的数据查看数据是否来了，若来了，则与其进行匹配
           //使用hashmap存储数据  key 表示关联的主键 id,value使用list来存储数据
            Map<Integer,List<Tuple2<Integer,String>>> s1cache =new HashMap<>();
            Map<Integer,List<Tuple3<Integer,String,Integer>>> s2chche =new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                if(!s1cache.containsKey(id)){
                    ArrayList<Tuple2<Integer, String>> s1values = new ArrayList<>();
                    s1values.add(value);
                    s1cache.put(id,s1values);
                }else {
                    s1cache.get(id).add(value);
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {

            }
        })*/

    }
}
