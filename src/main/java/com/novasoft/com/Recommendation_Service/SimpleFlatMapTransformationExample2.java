package com.novasoft.com.Recommendation_Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class SimpleFlatMapTransformationExample2 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample")
				.setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = jsc.parallelize(Arrays.asList("hello world","hi"));
		JavaRDD<String> world = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line){
				return Arrays.asList(line.split(" "));
			}
		});

		
		System.out.println("OUTPUT ==>");
		System.out.println(world.first());
		System.out.println(world.collect());
		
	}

}
