package com.commercial.base;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements  SourceFunction<Event>{
    private  Boolean running=true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users={"zhangsan","lisi","frog","second"};
        String[] url={"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running){
            sourceContext.collect(new Event(users[random.nextInt(users.length)],url[random.nextInt(url.length)], Calendar.getInstance().getTimeInMillis()));

        }

    }

    @Override
    public void cancel() {
        running=false;
    }
}
