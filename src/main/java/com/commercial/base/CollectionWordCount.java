package com.commercial.base;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectionWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(22);
        data.add(3);
        DataStreamSource<Integer> ds  = env.fromCollection(data);
        List<Integer> s = Arrays.asList(1, 22, 3);
        DataStreamSource<Integer> streamSource = env.fromCollection(s);


        //ds.print();
        streamSource.print();
        env.execute();

    }
}
