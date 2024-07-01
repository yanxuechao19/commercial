package com.commercial.Richfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/*实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join。App的支付事件和第三方的支付事件将会互相等待5秒钟，如果等不来对应的支付事件，那么就输出报警信息。*/
public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> con1 = env.fromElements(Tuple3.of("order-1", "app", 1000L), Tuple3.of("order-2", "app", 2000L)).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                return element.f2;
            }
        }));


        SingleOutputStreamOperator<Tuple3<String, String, Long>> con2 = env.fromElements(Tuple3.of("order-1", "third-party", 3000L), Tuple3.of("order-3", "third-party", 4000L)).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                return element.f2;
            }
        }));

        ConnectedStreams<Tuple3<String, String, Long>, Tuple3<String, String, Long>> connect3 = con1.connect(con2);
        SingleOutputStreamOperator<String> process = connect3.keyBy(new keyselect1(), new keyselect2()).process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
            //todo 声明状态用来保留app支付事件
            private ValueState<Tuple3<String, String, Long>> app;
            private ValueState<Tuple3<String, String, Long>> third_party;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                app = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
                third_party = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("third_party", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));

            }

            @Override
            public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                if (third_party != null) {
                    out.collect("对账成功" + value.f0);
                    third_party.clear();
                } else {
                    app.update(value);
                    ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                }
            }

            @Override
            public void processElement2(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                if (app != null) {
                    out.collect("对账成功" + value.f0);
                    app.clear();
                } else {
                    third_party.update(value);
                    ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                }
            }

            @Override
            public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                if (app.value() != null) {
                    out.collect("对账成功" + app);
                    app.clear();
                } else {
                    out.collect("对账失败" + app);
                }


                if (third_party.value() != null) {
                    out.collect("对账成功" + third_party);
                    third_party.clear();
                } else {

                    out.collect("对账失败" + third_party);
                }
/////////////////////////////////////////////////////////////////////////////////////////////////////


                /////////////////////////////////////////////////////////////////////////////////////////////////////


            }
        });
        process.print("对账结果：");
        env.execute();


    }
    public  static class keyselect1 implements KeySelector<Tuple3<String,String,Long>,String>{


        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }

    public  static class keyselect2 implements KeySelector<Tuple3<String,String,Long>,String>{


        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }


}
