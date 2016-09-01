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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SimpleSubstractTransformationExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//White List
		JavaRDD<String> whiteListRdd = jsc
				.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/whitelist.csv");
		
		//Convert file read into a Java pair Rdd
		JavaPairRDD<Integer, String> whiteListPairRdd = whiteListRdd.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				return new Tuple2(s.split(" ")[0], s);
			}
		});
		
		System.out.println("*******WhiteList Rdd: " + whiteListPairRdd.collect());				
		
		JavaRDD<String> whiteListRdd2 = jsc
				.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/whitelist-2.csv");
		
		//Convert file read into a Java pair Rdd
		JavaPairRDD<Integer, String> whiteListPairRdd2 = whiteListRdd2.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				return new Tuple2(s.split(" ")[0], s);
			}
		});
		
		System.out.println("*******WhiteList 2 Rdd: " + whiteListPairRdd2.collect());
		
		System.out.println("aa == >" + whiteListPairRdd.join(whiteListPairRdd2).collect());
		
		JavaPairRDD<Integer, Tuple2<String, String>> bb = whiteListPairRdd.join(whiteListPairRdd2);
		
		System.out.println("bb == >" + bb.collect());
		
		if(bb.isEmpty())
		{
			System.out.println("bb is empty");
		}
		else{
			System.out.println("bb is NOT empty");
		}
		
		
		JavaPairRDD<Integer, String> cc = bb.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<String,String>>, Integer, String>() {

			public Tuple2<Integer, String> call(Tuple2<Integer, Tuple2<String, String>> arg0) throws Exception {
				// TODO Auto-generated method stub
				Integer nn = arg0._1;
				String mm = arg0._2._1;
				return new Tuple2<Integer, String>(nn, mm);
			}
		});
		
		System.out.println("keys: "+bb.keys().collect());
		System.out.println("Values: "+bb.values().collect());

	}

}
