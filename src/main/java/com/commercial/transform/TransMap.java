package com.commercial.transform;

import com.commercial.base.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Calendar;

public class TransMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.fromElements(new Event("yxc", "./frog", Calendar.getInstance().getTimeInMillis()), new Event("hhd", "./second", Calendar.getInstance().getTimeInMillis()));
        //todo 匿名类实现
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        map.print();
        //todo
        SingleOutputStreamOperator<String> map1 = streamSource.map(new UserMap());
        map1.print();
        env.execute();
    }
    public static class UserMap implements MapFunction<Event,String>{

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
