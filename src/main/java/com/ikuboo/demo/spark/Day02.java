package com.ikuboo.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Day02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("day02");

        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("ERROR");


        JavaPairRDD<String, Integer> resut1 = context.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("zhangsan", 14),
                new Tuple2<String, Integer>("lisi", 30)));

        JavaPairRDD<String, Integer> resut2 = context.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("zhangsan", 24),
                new Tuple2<String, Integer>("wangwu", 14)));

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = resut1.join(resut2);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });
        System.out.println("-----------------------------");
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftOuterJoin = resut1.leftOuterJoin(resut2);
        leftOuterJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> stringTuple2Tuple2) throws Exception {
                    System.out.println(stringTuple2Tuple2);
                }
            }
        );

        JavaPairRDD<String, Integer> union = resut1.union(resut2);
        System.out.println("-----------------------------");
        union.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });


        context.close();
    }
}
