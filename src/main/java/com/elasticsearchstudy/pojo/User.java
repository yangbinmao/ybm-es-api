package com.elasticsearchstudy.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by YBM on 2020/12/7 0:38
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private  String name;
    private int age;
}
