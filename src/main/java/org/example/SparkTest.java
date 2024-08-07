package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkTest implements Serializable {

    private JavaSparkContext sc;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {

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

        // 保存
        stringRDD.saveAsTextFile("output");

        // 从外部创建
        JavaRDD<String> lineRdd = sc.textFile("input");
        List<String> result2 = lineRdd.collect();
        for (String s: result2){
            System.out.println(s);
        }

        // 从其他RDD创建

    }

    @Test
    public void transformation()  {
        // map
        JavaRDD<String> lineRDD = sc.textFile("input/a.txt");
        JavaRDD<String> maprdd = lineRDD.map(s -> s + "_hhh"); // lambda 表达式写法
        System.out.println(maprdd.collect());
        System.out.println(lineRDD.collect());

        // flatmap
        ArrayList<List<String>> arrayLists = new ArrayList<>();
        arrayLists.add(Arrays.asList("1","2","3"));
        arrayLists.add(Arrays.asList("4","5","6"));
        JavaRDD<List<String>> listJavaRDD = sc.parallelize(arrayLists,2);
        // 对于集合嵌套的RDD 可以将元素打散
        // 泛型为打散之后的元素类型
        JavaRDD<String> stringJavaRDD = listJavaRDD.flatMap(List::iterator);
        stringJavaRDD. collect().forEach(System.out::println);

    }



}
