package com.commercial.Richfunction;

import com.commercial.base.ClickSource;
import com.commercial.base.ClickUnionSource;
import com.commercial.base.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
//todo 简单的合流操作，就是直接将多条流合在一起，叫作流的“联合”（union）。联合操作要求必须流中的数据类型必须相同，合并之后的新流会包括所有流中的元素，数据类型不变。
//todo 这里需要考虑一个问题。在事件时间语义下，水位线是时间的进度标志；不同的流中可能水位线的进展快慢完全不同，如果它们合并在一起，水位线又该以哪个为准呢？//还以要考虑水位线的本质含义，是“之前的所有数据已经到齐了”；所以对于合流之后的水位线，也是要以最小的那个为准，这样才可以保证所有流都不会再传来之前的数据。换句话说，多流合并时处理的时效性是以最慢的那个流为准的。
/*todo 水位线
*todo forMonotonousTimestamps{当数据流中的事件时间戳是单调递增的，即事件是按照事件时间的顺序产生的。
特点：
不需要等待时间或设置最大乱序度，因为数据本身就是有序的。
水位线是基于当前处理到的最大事件时间戳生成的。
在有序流中，水位线可以高效地推进，因为没有乱序数据的干扰。}
todo  forBoundedOutOfOrderness{当数据流中的事件时间戳可能出现乱序，但乱序的程度是有限的。
特点：
需要设置最大乱序度（maxOutOfOrderness），表示允许事件时间戳与当前最大时间戳之间的最大差值。
水位线是基于当前处理到的最大事件时间戳减去最大乱序度生成的。
这种策略允许在等待乱序数据到达的同时，也能继续推进水位线，以减少延迟。}
todo  forGenerator{适用场景：当需要基于特定的业务逻辑或需求来生成水位线时。
特点：
完全由用户自定义生成逻辑。
可能需要结合其他数据源或系统状态来生成水位线。
灵活性高，但实现复杂度也可能较高。}
* */
//todo  当前代码的水位线设置还是存在一些问题 返回值为 -1 说明水位线设置存在问题，此处不做纠结，主要了解合流
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource1 = env.addSource(new ClickSource());
        DataStreamSource<Event> streamSource2 = env.addSource(new ClickUnionSource());
        SingleOutputStreamOperator<Tuple2<String, Long>> stram1 = streamSource1.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, value.timestamp);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return 0;
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Long>> stram2 = streamSource2.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, value.timestamp);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return 0;
            }
        }));
        stram1.union(stram2).process(new ProcessFunction<Tuple2<String, Long>, String>() {
            @Override
            public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("当前水位线是： "+ ctx.timerService().currentWatermark());
            }
        }).print();
        env.execute();


    }
}
