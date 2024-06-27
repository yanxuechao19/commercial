package com.commercial.transform;

import com.commercial.base.ClickSource;
import com.commercial.base.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// todo 从用户的点击数据中筛选包含“home”的内容：
public class TransFunctionUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        // 1
        SingleOutputStreamOperator<Event> filter1 = streamSource.filter(new filterfunction());
        //filter1.print();
        //2
        SingleOutputStreamOperator<Event> filter = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.url.contains("home");
            }
        });
        //filter.print();
        //3
        SingleOutputStreamOperator<Event> filter2 = streamSource.filter(new keywordfilter("home"));
        filter2.print();
        env.execute();

    }

    public  static class filterfunction implements  FilterFunction<Event>{
        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains("home");
        }
    }
    public  static  class keywordfilter implements FilterFunction<Event>{
        private String keyword;

        public keywordfilter(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public boolean filter(Event value) throws Exception {

            return value.url.contains(this.keyword);
        }
    }


}
