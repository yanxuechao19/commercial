package com.commercial.base;

import java.sql.Timestamp;
import java.util.Objects;

//todo Flink对POJO类型的要求如下：
//类是公有（public）的
//有一个无参的构造方法
//所有属性都是公有（public）的 或者属性私有，但是必须提供public的get/set方法
//所有属性的类型都是可以序列化的
//如果属性在外部使用 则还需要一个全参的构造器
public class Event {
    public  String user;
    public  String url;
    public  Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(user, event.user) && Objects.equals(url, event.url) && Objects.equals(timestamp, event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, url, timestamp);
    }
}
