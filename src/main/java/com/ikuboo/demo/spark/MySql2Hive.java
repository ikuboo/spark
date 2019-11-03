package com.ikuboo.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public class MySql2Hive {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local")
                .appName("SparkHive")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .config("hive.metastore.uris","thrift://127.0.0.1:9083")
                .config("spark.sql.warehouse.dir","hdfs://127.0.0.1:9000/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        Map<String, String> options = new HashMap<String,String>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/or_test");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "root");
        options.put("dbtable", "dep");
        //读取mysql
        Dataset<Row> dataset = spark.read().format("jdbc").options(options).load();
        dataset.show();


        spark.sql("CREATE DATABASE IF NOT EXISTS ikuboo_test").show();
        spark.sql("use ikuboo_test");

        //写hive
        dataset.write().mode(SaveMode.Overwrite).saveAsTable("dep");
        Dataset<Row> hive_dep = spark.sql("select * from dep");
        hive_dep.show();

        spark.stop();
    }
}
