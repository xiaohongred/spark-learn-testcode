package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
public class SparkCoreExample {

    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        ArrayList<List<String>> arrayLists = new ArrayList<>();

        arrayLists.add(Arrays.asList("1","2","3"));
        arrayLists.add(Arrays.asList("4","5","6"));


        JavaRDD<List<String>> listJavaRDD = sc.parallelize(arrayLists,2);
        // 对于集合嵌套的RDD 可以将元素打散
        // 泛型为打散之后的元素类型
        JavaRDD<String> stringJavaRDD = listJavaRDD.flatMap(new FlatMapFunction<List<String>, String>() {
            @Override
            public Iterator<String> call(List<String> strings) throws Exception {
                return strings.iterator();
            }
        });

        stringJavaRDD.collect().forEach(System.out::println);

        // 通常情况下需要自己将元素转换为集合
        JavaRDD<String> lineRDD = sc.textFile("input/a.txt");

        JavaRDD<String> stringJavaRDD1 = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Arrays.asList(s1).iterator();
            }
        });

        stringJavaRDD1.collect().forEach(System.out::println);


        // groupBy 分组
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4),2);
        JavaPairRDD<Integer, Iterable<Integer>> groupByRdd = integerJavaRDD.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 % 2;
            }
        });
        groupByRdd.collect().forEach(System.out::println);
        // 类型可以任意修改
        JavaPairRDD<Boolean, Iterable<Integer>> groupByRDD1 = integerJavaRDD.groupBy(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        groupByRDD1.collect().forEach(System.out::println);


        // filter 过滤
        JavaRDD<Integer> integerJavaRDDFilter = sc.parallelize(Arrays.asList(1, 2, 3, 4),2);
        JavaRDD<Integer> filterRDD = integerJavaRDDFilter.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        filterRDD.collect().forEach(System.out::println);

        // distinct 去重
        JavaRDD<Integer> integerJavaRDDDis = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 6, 6,6,6,6), 2);
        System.out.println("before distinct");
        integerJavaRDDDis.collect().forEach(System.out::println);
        System.out.println("after distinct");
        // 底层使用分布式分组去重  所有速度比较慢,但是不会OOM
        JavaRDD<Integer> distinct = integerJavaRDDDis.distinct();
        distinct.collect().forEach(System.out::println);

        // sortBy
        JavaRDD<Integer> integerJavaRDDSortBy = sc.parallelize(Arrays.asList(5, 8, 1, 11, 20), 2);
        // (1)泛型为以谁作为标准排序  (2) true为正序  (3) 排序之后的分区个数
        JavaRDD<Integer> sortByRDD = integerJavaRDDSortBy.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 2);
        sortByRDD.collect().forEach(System.out::println);



        // mapValues
        JavaRDD<Integer> integerJavaRDDmapValues = sc.parallelize(Arrays.asList(1, 2, 3, 4),2);

        JavaPairRDD<Integer, Integer> pairRDD = integerJavaRDDmapValues.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer + 10);
            }
        });
        pairRDD.collect().forEach(System.out::println);

        JavaPairRDD<String, String> javaPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("k", "v"), new Tuple2<>("k1", "v1"), new Tuple2<>("k2", "v2")));
        JavaPairRDD<String, String> mapValuesRDD = javaPairRDD.mapValues(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + "|||";
            }
        });
        mapValuesRDD.collect().forEach(System.out::println);

        // groupByKey
        JavaRDD<String> integerJavaRDDgroupByKey = sc.parallelize(Arrays.asList("hi","hi","hello","spark" ),2);

        JavaPairRDD<String, Integer> pairRDDgroupByKey = integerJavaRDDgroupByKey.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 聚合相同的key
        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = pairRDDgroupByKey.groupByKey();
        groupByKeyRDD.collect().forEach(System.out::println);
        // 合并值
        JavaPairRDD<String, Integer> result = groupByKeyRDD.mapValues(new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> v1) throws Exception {
                Integer sum = 0;
                for (Integer integer : v1) {
                    sum += integer;
                }
                return sum;
            }
        });
        result.collect().forEach(System.out::println);

        // reduceByKey
        JavaRDD<String> integerJavaRDDreduceByKey = sc.parallelize(Arrays.asList("hi","hi","hello","spark" ),2);
        // 统计单词出现次数
        JavaPairRDD<String, Integer> pairRDDreduceByKey = integerJavaRDDreduceByKey.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        // 聚合相同的key
        JavaPairRDD<String, Integer> resultreduceByKey = pairRDDreduceByKey.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        resultreduceByKey.collect().forEach(System.out::println);

        // sortByKey
        JavaPairRDD<Integer, String> javaPairRDDsortByKey = sc.parallelizePairs(Arrays.asList(new Tuple2<>(2, "d"),new Tuple2<>(4, "a"), new Tuple2<>(3, "c")));
        // 填写布尔类型选择正序倒序
        JavaPairRDD<Integer, String> pairRDDsortByKey = javaPairRDDsortByKey.sortByKey(false);
        System.out.println("sortByKey example output:");
        pairRDDsortByKey.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();
    }

}
