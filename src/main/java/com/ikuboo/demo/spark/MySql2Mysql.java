package com.ikuboo.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MySql2Mysql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("MySql2Mysql");
        /**
         * 	配置join或者聚合操作shuffle数据时分区的数量
         */
        conf.set("spark.sql.shuffle.partitions", "1");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        Map<String, String> options = new HashMap<String,String>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/or_test");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "root");
        options.put("dbtable", "dep");

        Dataset<Row> dataset1 = sqlContext.read().format("jdbc").options(options).load();
        dataset1.show();

        dataset1.createOrReplaceTempView("dep_view");
        Dataset<Row> dataset2 = sqlContext.sql("select dep_name from dep_view where dep_id > 20");
        dataset2.show();
        /**
         * 将DataFrame结果保存到Mysql中
         */
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        /**
         * SaveMode:
         * Overwrite：覆盖
         * Append:追加
         * ErrorIfExists:如果存在就报错
         * Ignore:如果存在就忽略
         *
         */
        dataset2.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/or_test", "dataset2", properties);
        System.out.println("----Finish----");
        sc.stop();
    }
}
