package com.novasoft.com.Recommendation_Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SimpleJoinExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> left = new ArrayList<Tuple2<String, Integer>>();
		left.add(new Tuple2<String, Integer>("A", 0));
		left.add(new Tuple2<String, Integer>("B", 0));
		left.add(new Tuple2<String, Integer>("C", 0));
		left.add(new Tuple2<String, Integer>("D", 0));
		left.add(new Tuple2<String, Integer>("E", 0));

		List<Tuple2<String, Integer>> right = new ArrayList<Tuple2<String, Integer>>();
		right.add(new Tuple2<String, Integer>("B", 1));
		right.add(new Tuple2<String, Integer>("C", 5));
		right.add(new Tuple2<String, Integer>("D", 21));

		JavaPairRDD<String, Integer> leftRdd = jsc.parallelizePairs(left);
		JavaPairRDD<String, Integer> rightRdd = jsc.parallelizePairs(right);

//		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> lojRdd = leftRdd.leftOuterJoin(rightRdd);
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> lojRdd = leftRdd.leftOuterJoin(rightRdd);

		JavaPairRDD<String, Integer> result = lojRdd.mapValues(new Function<Tuple2<Integer, Optional<Integer>>, Integer>() {
		  public Integer call(Tuple2<Integer, Optional<Integer>> v1) throws Exception {
		    return v1._2().or(v1._1());
		  }
		});

		result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
		  public void call(Tuple2<String, Integer> t) throws Exception {
		    System.out.println(t._1() + " " + t._2());
		  }
		});

	}
	}
