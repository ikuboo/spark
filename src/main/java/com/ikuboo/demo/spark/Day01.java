package com.ikuboo.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 转换算子和存储算子
 */
public class Day01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("day01");

        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("ERROR");
        JavaRDD<String> lines = context.textFile("src/data/words.txt");

        //map 一对一
//        JavaRDD<String> result = lines.map(new Function<String, String>() {
//            public String call(String s) throws Exception {
//                return s + "!!!";
//            }
//        });
//       filter 过滤
//       JavaRDD<String> result = lines.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) throws Exception {
//                return s.endsWith("spark");
//            }
//        });


        //flatMap 一对多
        JavaRDD<String> result1 = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        //mapToPair 一对一个k-v
        JavaPairRDD<String, Integer> result2 = result1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //reduceByKey 按照key聚合
        JavaPairRDD<String, Integer> result3 = result2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //置换k-v
        JavaPairRDD<Integer, String> result4 = result3.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });

        //按照key排序
        JavaPairRDD<Integer, String> result5 = result4.sortByKey(false);

        JavaPairRDD<String, Integer> result6 = result5.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
            }
        });

        System.out.println(result6.count());
        result6.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            public void call(Tuple2<String,Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });

        context.close();
    }
}
