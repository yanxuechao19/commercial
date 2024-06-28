package com.commercial.Richfunction;

import com.commercial.base.ClickSource;
import com.commercial.base.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// todo 将电商网站收集到的用户行为数据进行一个拆分，根据类型（type）的不同，分为“frog”的浏览数据、“second”的浏览数据等。
/*todo ctx.output(...):这是 ProcessFunction 上下文（Context）的一个方法，用于将结果发送到侧输出流（side output）。在 Flink 中，侧输出流允许你将数据流分割成多个逻辑上不同的流，每个流都可以独立地处理。*/
/*todo out.collect(...):这是 Collector 接口的一个方法，用于将结果发送到主输出流。在 Flink 的大多数操作符中，主输出流是默认的输出流，除非你明确指定了侧输出流。*/
public class SplitStreamByFilter {
    //匿名类写法1 直接声明泛型
    private static OutputTag<Tuple3<String,String, Long>> frogTag = new OutputTag<Tuple3<String,String, Long>>("frog-late-data"){};
    private static  OutputTag<Tuple3<String,String, Long>> secondTag = new OutputTag<Tuple3<String,String, Long>>("second-late-data"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        //方法一 侧输出流标签使用匿名内部类
        SingleOutputStreamOperator<Event> process = streamSource.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("frog")) {
                    //todo 将数据发到测输出流，要传入 侧输出流标签和侧输出流接收数据的对象
                   ctx.output( frogTag,new Tuple3<>(value.user,value.url, value.timestamp));
                } else if (value.user.equals("second")) {
                    ctx.output( secondTag,new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

        process.getSideOutput(frogTag).print("frog-pv");
        SideOutputDataStream<Tuple3<String, String, Long>> secondtag = process.getSideOutput(secondTag);
        // OutputTag<Tuple3<String,String, Long>> secondTag
        secondtag.print();

        //方法二 侧输出流标签使用匿名内部类的重载方法
        //直接使用OutputTag重载的方法来实现
        OutputTag<Event> frog = new OutputTag<Event>("frog", Types.POJO(Event.class));
        OutputTag<Event> second = new OutputTag<Event>("second", Types.POJO(Event.class));
        SingleOutputStreamOperator<Event> process1 = streamSource.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("frog")) {
                    ctx.output(frog, value);
                } else if (value.user.equals("second")) {
                    ctx.output(second, value);
                } else {
                    out.collect(value);
                }
            }
        });
        process1.getSideOutput(frog).print("frog2");//todo 侧输出流尽量写名字 ，不然不知道是谁输出的
        process1.getSideOutput(frog).print("second2");



        env.execute();


    }
}
