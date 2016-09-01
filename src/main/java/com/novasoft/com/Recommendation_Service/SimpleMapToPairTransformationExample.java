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

public class SimpleMapToPairTransformationExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//1. White List
		JavaRDD<String> whiteListRdd = jsc.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/whitelist.csv");

		//Convert file read into a Java pair Rdd
		JavaPairRDD<String, String> whiteListPairRdd = whiteListRdd.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				return new Tuple2(s.split(" ")[0], s);
			}
		});
		
		//2. Read data from file
		String trainedDataPath= "/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/AlsTrainedSampleData";
		
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), trainedDataPath);	

		System.out.println("*******WhiteList Rdd: " + whiteListPairRdd.collect());
		
		JavaRDD<Tuple2<Object, Rating[]>> ratings3 = sameModel.recommendProductsForUsers(3).toJavaRDD();
		//System.out.println("************Number of records in ratings 3:"+ratings3.count()+"*************");			
		
		JavaPairRDD<Integer, String> ratings1 = ratings3.mapToPair(new PairFunction<Tuple2<Object,Rating[]>, Integer, String>() {
			//int a=0;				
			public Tuple2<Integer, String> call(Tuple2<Object, Rating[]> entry) throws Exception {
			/*	Rating[] userAndProducts = entry._2;
				for (Rating r : userAndProducts) {	
					a = r.product();
					r.toString();
				}	*/										
				return new Tuple2<Integer, String>(14,"14");
			}
		});
		
		
		JavaPairRDD<String, Rating> recommConversion = ratings3.mapToPair(new PairFunction<Tuple2<Object,Rating[]>, String, Rating>() {

			public Tuple2<String, Rating> call(Tuple2<Object, Rating[]> arg0) throws Exception {
				Rating[] userAndProducts = arg0._2;
				String itemId = null;
				int userId = 0;
				for(Rating r : userAndProducts){
					itemId = String.valueOf(r.product());
					userId = r.user();
				}
				// TODO Auto-generated method stub
				return new Tuple2<String,Rating>(itemId, new Rating(userId, 2, 3.0));
			}
		});
						
		
		JavaPairRDD<Integer, Rating> recommConversion1 = ratings3.mapToPair(new PairFunction<Tuple2<Object,Rating[]>, Integer, Rating>() {

			public Tuple2<Integer, Rating> call(Tuple2<Object, Rating[]> arg0) throws Exception {
				Rating[] userAndProducts = arg0._2;
				int itemId = 0;
				int userId = 0;
				double score = 0;
				for(Rating r : userAndProducts){
					itemId = r.product();
					userId = r.user();
					score = r.rating();
				}
				// TODO Auto-generated method stub
				return new Tuple2<Integer,Rating>(itemId, new Rating(userId, itemId, score));
			}
		});

		
		
		System.out.println("******RecommConversion"+ recommConversion.collect());
		
		System.out.println("Joined "+ recommConversion.join(whiteListPairRdd).collect());
		
		
		//=========================================================
		/*JavaPairRDD<Integer, String> whiteListPairRdd2 = whiteListRdd2.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				return new Tuple2(s.split(" ")[0], s);
			}
		});
		
		JavaPairRDD<Integer, Tuple2<String, String>> bb = whiteListPairRdd.join(whiteListPairRdd2);*/
		//=========================================================		
	
		
		System.out.println("*******ratings1 Rdd: " + ratings1.take(2));				
				
		//JavaPairRDD<Integer, Tuple2<String, String>> joinedData = ratings1.join(whiteListPairRdd);
		
		//System.out.println("aa == >" + joinedData.collect());
		
		//if(joinedData.isEmpty())
		//{
		//	System.out.println("aa is empty");
		//}
		
	/*	JavaRDD<String> whiteListRdd2 = jsc
				.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/whitelist-2.csv");
		
		//Convert file read into a Java pair Rdd
		JavaPairRDD<Integer, String> whiteListPairRdd2 = whiteListRdd2.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				return new Tuple2(s.split(" ")[0], s);
			}
		});
		
		System.out.println("*******WhiteList 2 Rdd: " + whiteListPairRdd2.collect());
		
		System.out.println("aa == >" + whiteListPairRdd.join(whiteListPairRdd2).collect());
		
		JavaPairRDD<Integer, String> bb = whiteListPairRdd.subtract(whiteListPairRdd2);
		
		System.out.println("bb == >" + bb.collect());
		
		if(bb.isEmpty())
		{
			System.out.println("bb is empty");
		}
		else{
			System.out.println("bb is NOT empty");
		}*/
	
	}

}
