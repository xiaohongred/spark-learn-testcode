package org.example;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class SparkTest {

    private JavaSparkContext sc;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {

        Configuration config = new Configuration();

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        sc = new JavaSparkContext(conf);
    }

    @After
    public void close() throws IOException {
        // 4. 关闭sc
        sc.stop();
    }


    @Test
    public void createRdd() {

        // 从列表创建
        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("hello", "spark"));
        List<String> result = stringRDD.collect();
        for (String s : result) {
            System.out.println(s);
        }


        // 从外部创建
        JavaRDD<String> lineRdd = sc.textFile("input");
        List<String> result2 = lineRdd.collect();
        for (String s: result2){
            System.out.println(s);
        }

        // 从其他RDD创建


    }


}
