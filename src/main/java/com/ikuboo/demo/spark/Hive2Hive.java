package com.ikuboo.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Hive2Hive {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local")
                .appName("SparkHive")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .config("hive.metastore.uris","thrift://127.0.0.1:9083")
                .config("spark.sql.warehouse.dir","hdfs://127.0.0.1:9000/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();


        spark.sql("show databases").show();
        spark.sql("use ikuboo_test");
        spark.sql("show tables").show();

        Dataset<Row> hive_dep = spark.sql("select * from dep");
        hive_dep.show();
        //将数据通过覆盖的形式保存在数据表中
        hive_dep.write().mode(SaveMode.Overwrite).saveAsTable("dep_2");

        Dataset<Row> hive_dep_2 = spark.sql("select * from dep_2");
        hive_dep_2.show();
        spark.close();
    }
}
