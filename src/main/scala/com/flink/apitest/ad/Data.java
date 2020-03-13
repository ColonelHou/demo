package com.flink.apitest.ad;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Data {
    public static void main(String[] args) throws Exception {
        BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(new File("./AdClickLog.csv"))));
        List<String> province = new ArrayList<String>();
        province.add("beijing");
        province.add("shanghai");
        province.add("guangdong");
        province.add("shanxi");
        province.add("tianjin");
        List<String> city = new ArrayList<String>();
        city.add("beijing");
        city.add("shenzhen");
        city.add("zhongshan");
        city.add("guangzhou");
        city.add("xian");
        city.add("tianjin");
        System.out.println((int)((Math.random()*9+1)*100000));


        for (int i = 0; i < 10000; i ++) {
            bw.write((int)((Math.random()*9+1)*100000) + ","
                    + (int)((Math.random()*9+1)*1000) + ","
                    + province.get(new Random().nextInt(province.size())) + ","
                    + city.get(new Random().nextInt(city.size())) + ","
                    + (System.currentTimeMillis() / 1000 + new Random().nextInt(10000)) + "\n");
        }
        bw.close();
    }
}
