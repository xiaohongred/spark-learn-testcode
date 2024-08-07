package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
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
public class SparkSqlExample {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 按照行读取
        Dataset<Row> lineDS = spark.read().json("input/user.json");

        Dataset<User> userDS = lineDS.as(Encoders.bean(User.class));
        userDS.show();

        Dataset<User> userDataset = lineDS.map(
                new MapFunction<Row, User>() {
                   @Override
                   public User call(Row value) throws Exception {
                       return new User(value.getLong(0), value.getString(1));
                   }
               }, Encoders.bean(User.class)
        );
        userDataset.show();


        spark.close();

    }
}
