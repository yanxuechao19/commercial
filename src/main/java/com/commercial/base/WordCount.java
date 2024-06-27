package com.commercial.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data//用于生成Java Bean所需的所有方法，例如equals、hashCode、toString和getters/setters等方法。注解加在类上时，会自动为类的所有属性添加getter和setter方法，此外还会自动生成equals、hashCode、toString、constructor等方法。
@NoArgsConstructor//用于生成无参构造函数，默认生成的构造函数是public权限的。
@AllArgsConstructor//用于生成全参构造函数，默认生成构造函数的访问权限也是public。注解加在类上时，会为所有字段生成构造函数。
public class WordCount {
    private  String word;
    private  Integer count;
}
