package com.commercial.transform;

import com.commercial.base.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Calendar;

public class TransFlatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.fromElements(new Event("yxc", "./frog", Calendar.getInstance().getTimeInMillis()), new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()),new Event("yxc", "./prod", Calendar.getInstance().getTimeInMillis()));
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = streamSource.flatMap(new UserFlatmap() {
        });
        stringSingleOutputStreamOperator.print();
        env.execute();
    }
    public  static  class  UserFlatmap implements  FlatMapFunction<Event,String>{

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {

            if(event.user.equals("yxc")){
                collector.collect(event.url);
            }else if(event.user.equals("dtt"))  {
                collector.collect(event.toString());
            }
        }
    }

}
