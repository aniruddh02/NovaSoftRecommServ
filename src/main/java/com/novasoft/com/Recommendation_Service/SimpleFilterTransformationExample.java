package com.novasoft.com.Recommendation_Service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class SimpleFilterTransformationExample {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SimpleTranformExample")
				.setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> inputRdd = jsc.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/ScratchPad.txt");
		JavaRDD<String> sparkSubmitLines = inputRdd.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.contains("spark-submit");
			}
		});
		System.out.println("OUTPUT IS::=>");
		System.out.println(sparkSubmitLines.collect());
		
	}
}
