package com.commercial.transform;

import com.commercial.base.Event;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Calendar;

public class TransFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.fromElements(new Event("yxc", "./frog", Calendar.getInstance().getTimeInMillis()), new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()));
        SingleOutputStreamOperator<Event> filter = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                /*因为 == 操作符用于比较引用而不是内容，所以即使 event.user.toString() 返回的字符串内容与 "yxc" 相同，这两个字符串对象在内存中的引用也是不同的，因此比较结果将为 false*//*

              if(event.user.toString()=="yxc"){
                  return true;
              }else {
                  return false;
              }*/
                return  event.user.equals("yxc");
            }
        });
        filter.print();
        SingleOutputStreamOperator<Event> filter1 = streamSource.filter(new UserFilter());
        //filter1.print();
        env.execute();

    }
    public  static class UserFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return  event.user.equals("yxc");
        }
    }

}
