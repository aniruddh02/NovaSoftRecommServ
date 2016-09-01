package com.novasoft.com.Recommendation_Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleMapTransformationExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample")
				.setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,4));
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			public Integer call(Integer x){
				return x*x;
			}
		});
		
		System.out.println("OUTPUT ==>");
		System.out.println(result.collect());
		
	}

}
