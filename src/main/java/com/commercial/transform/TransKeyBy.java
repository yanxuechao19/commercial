package com.commercial.transform;
//todo 在内部，是通过计算key的哈希值（hash code），对分区数进行取模运算来实现的。所以这里key如果是POJO的话，必须要重写hashCode()方法。
//todo keyBy()方法需要传入一个参数，这个参数指定了一个或一组key
/*todo 比如对于Tuple数据类型，可以指定字段的位置或者多个位置的组合；对于POJO类型，可以指定字段的名称（String）；另外，还可以传入Lambda表达式或者实现一个键选择器（KeySelector），用于说明从数据中提取key的逻辑*/
//todo 在内部，是通过计算key的哈希值（hash code），对分区数进行取模运算来实现的。所以这里key如果是POJO的话，必须要重写hashCode()方法
import com.commercial.base.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Calendar;

public class TransKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.fromElements(new Event("yxc", "./frog", Calendar.getInstance().getTimeInMillis()), new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()),new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()),new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()),new Event("yxc", "./prod", Calendar.getInstance().getTimeInMillis()),new Event("dtt", "./prod", Calendar.getInstance().getTimeInMillis()));
        KeyedStream<Event, String> keyed = streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });
        env.execute();


    }

}
