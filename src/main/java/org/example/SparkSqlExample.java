package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
public class SparkSqlExample {
    public static final String CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
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

        Dataset<Row> mysqlDf = spark.read().format("jdbc")
                .option("driver", MYSQL_DRIVER)
                .option("url", AppConfig.getProperties().getProperty("mysql.url"))
                .option("user", AppConfig.getProperties().getProperty("mysql.user"))
                .option("password", AppConfig.getProperties().getProperty("mysql.passwd"))
                .option("query", "select * from s1")
                .load();
        mysqlDf.show();
        // 创建临时视图
        mysqlDf.createOrReplaceTempView("mysql_tmp_s1");
        Dataset<Row> filterdf = mysqlDf.filter(col("key1").equalTo("xLaBmcAvXQ"));

        filterdf.show();

        filterdf.write()
                .format("jdbc")
                .mode("overwrite")
                .option("url", AppConfig.getProperties().getProperty("mysql.url"))
                .option("user", AppConfig.getProperties().getProperty("mysql.user"))
                .option("password", AppConfig.getProperties().getProperty("mysql.passwd"))
                .option("dbtable", "db_test.spark_test_table")
                .save();


        UserDefinedFunction addName = udf(new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s + "_xiaohonghhhh";
            }
        }, DataTypes.StringType);

        spark.udf().register("addName", addName);
        spark.sql("select addName(key1) from mysql_tmp_s1").show();

        spark.close();
    }
}
