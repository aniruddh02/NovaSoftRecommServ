package com.novasoft.com.Recommendation_Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.tools.nsc.doc.model.comment.Paragraph;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

public class SparkHbaseReadExample {
	public static final Logger orglogger = Logger.getLogger("org");
	public static final Logger comLogger = Logger.getLogger("com");

	public static final String STARTOFSCORING = "Scoring Process start#";
	public static final String ENDOFSCORING = "Scoring process ended#"; 

	public static void main(String[] args) {
		String responseStr = "";
		try {
			
			//Context and configuration begin
			SparkConf conf = new SparkConf().setAppName("scoring").setMaster("local");

			JavaSparkContext jsc = new JavaSparkContext(conf);
			//Context and configuration end			
			
			//White List
			JavaRDD<String> whiteListRdd = jsc.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/whitelistScoring.csv");
			
			//Convert file read into a Java pair Rdd
			JavaPairRDD<String, String> whiteListPairRdd = whiteListRdd.mapToPair(new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(String s) {
					return new Tuple2(s.split(" ")[0], s);
				}
			});
			
			JavaRDD<String> blackListRdd = jsc
					.textFile("/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/While-Black List Sample Files/blacklistScoring.csv");
			
			//Convert file read into a Java pair Rdd
			JavaPairRDD<String, String> blackListPairRdd = blackListRdd.mapToPair(new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(String s) {
					return new Tuple2(s.split(" ")[0], s);
				}
			});
			
			JavaPairRDD<String, String> whiteBlackListFilter = whiteListPairRdd.subtract(blackListPairRdd);
			System.out.println("finalFilter=>" + whiteBlackListFilter.collect());
			
			
			//Hbase Configuration begin
			System.out.println("************Hbase config start*************");
			Configuration config = null;
			try {
			       config = HBaseConfiguration.create();
			       config.set("hbase.zookeeper.quorum", "10.219.6.201");
			       config.set("hbase.zookeeper.property.clientPort","2181");
			       HBaseAdmin.checkHBaseAvailable(config);
			       System.out.println("HBase is running!");			       			     
			     } 
			catch (MasterNotRunningException e) {
			            System.out.println("HBase is not running!");
			            System.exit(1);
			}catch (Exception ce){ 
			        ce.printStackTrace();
			}
			
			System.out.println("************Hbase config end*************");
			
			//Read Items lookup Map
			config.set(TableInputFormat.INPUT_TABLE, "item_E98495DF15A740DB807B214ADAC45E37_reverse");
			config.set(TableInputFormat.SCAN_COLUMN_FAMILY, "lookup");			
			config.set(TableInputFormat.SCAN_COLUMNS, "lookup:lookup_id");
			//Hbase Configuration end
			
			JavaPairRDD<ImmutableBytesWritable, Result> itemHbaseMapRdd = 
			          jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
			//Item Table <itemValue, id>
			JavaPairRDD<String, Integer> itemMapLookupPairRdd = itemHbaseMapRdd.mapToPair(
			        new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
			        @Override
			    public Tuple2<String, Integer> call(
			        Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
			         
			           Result r = entry._2;
			          String keyRow = Bytes.toString(r.getRow());
			          String value = Bytes.toString(r.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("lookup_id")));			          
			          
			        return new Tuple2<String, Integer>(keyRow, Integer.valueOf(value));
			    }
			});		
			
			
			System.out.println("************persisting Java Rdd*************");
			itemMapLookupPairRdd.persist(StorageLevel.MEMORY_AND_DISK());								
			
			//Shortlisting items to calcualte score on
			JavaPairRDD<String, Tuple2<String, Integer>> filteredProducts = whiteBlackListFilter.join(itemMapLookupPairRdd);
			
			System.out.println("filteredProducts == >" + filteredProducts.collect());
									
			//item table reverse <id, itemValue>				
			JavaPairRDD<Integer, String> itemHbaseMapLookupRddReverse = filteredProducts.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Integer>>, Integer, String>() {

				@Override
				public Tuple2<Integer, String> call(Tuple2<String, Tuple2<String, Integer>> arg0) throws Exception {
					return new Tuple2<Integer, String>(arg0._2()._2, arg0._1);
				}			
			});

			/*System.out.println("itemMapLookupPairRdd: " + itemMapLookupPairRdd.count());
			System.out.println("itemMapLookupPairRdd: " + itemMapLookupPairRdd.collect());
			System.out.println("itemMapLookupPairRdd Reverse: " + itemHbaseMapLookupRddReverse.count());
			System.out.println("itemMapLookupPairRdd Reverse: " + itemHbaseMapLookupRddReverse.collect());*/
			
			//Read Profile Lookup Map
			config.set(TableInputFormat.INPUT_TABLE, "profile_E98495DF15A740DB807B214ADAC45E37_reverse");
			config.set(TableInputFormat.SCAN_COLUMN_FAMILY, "lookup");			
			config.set(TableInputFormat.SCAN_COLUMNS, "lookup:lookup_id");
						
			JavaPairRDD<ImmutableBytesWritable, Result> userHbaseMapRdd = 
			          jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
			
			//User <userValue, id>	
			JavaPairRDD<String, Integer> userMapLookupPairRdd = userHbaseMapRdd.mapToPair(
			        new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
			        @Override
			    public Tuple2<String, Integer> call(
			        Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
			         
			           Result r = entry._2;
			           String keyRow = Bytes.toString(r.getRow());
			          String value = Bytes.toString(r.getValue(Bytes.toBytes("lookup"), Bytes.toBytes("lookup_id")));			         
			        return new Tuple2<String, Integer>(keyRow, Integer.valueOf(value));
			    }
			});
						
			//User Reverse <id, userValue>
			JavaPairRDD<Integer,String> userMapLookupPairRddReverse = userMapLookupPairRdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

				@Override
				public Tuple2<Integer, String> call(Tuple2<String, Integer> arg0) throws Exception {
					return new Tuple2<Integer,String>(arg0._2, arg0._1);
				}
			});
			
			/*System.out.println("profileMapLookupPairRdd: "+ userMapLookupPairRdd.count());
			System.out.println("profileMapLookupPairRdd: "+ userMapLookupPairRdd.collect());
			System.out.println("profileMapLookupPairRdd Reverse: "+ userMapLookupPairRddReverse.count());
			System.out.println("profileMapLookupPairRdd Reverse: "+ userMapLookupPairRddReverse.collect());	*/
			
			//Load MFM model data from FileSystem
			MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), "/Users/antiwari/Documents/Epsilon/Machine-Intelligence/MI_Docs/AlsTrainedSampleData");
			System.out.println("*******WhiteList Rdd: " + whiteListPairRdd.collect());			
			JavaRDD<Tuple2<Object, Rating[]>> ratings3 = sameModel.recommendProductsForUsers(3).toJavaRDD();
			
			//DEBUG Transform data for join operation
		/*	JavaPairRDD<String, Rating[]> recommConversion = ratings3.mapToPair(new PairFunction<Tuple2<Object,Rating[]>, String, Rating[]>() {

				public Tuple2<String, Rating[]> call(Tuple2<Object, Rating[]> arg0) throws Exception {
					Rating[] userAndProducts = arg0._2;
					String itemId = null;
					int userId = 0;
					double rating = 0;
					for(Rating r : userAndProducts){
						itemId = String.valueOf(r.product());
						userId = r.user();
						rating = r.rating();
					}			
					
					return new Tuple2<String,Rating[]>(itemId, arg0._2);
				}
			});*/
			
			JavaPairRDD<Integer, Rating> recommConversion2 = ratings3.flatMapToPair(new PairFlatMapFunction<Tuple2<Object,Rating[]>, Integer, Rating>() {
				@Override
				public Iterable<Tuple2<Integer, Rating>> call(Tuple2<Object, Rating[]> arg0) throws Exception {
					
					List<Tuple2<Integer, Rating>> intRating = new ArrayList();
					Rating[] userAndProducts = arg0._2;
					Integer itemId = null;
					for(Rating r : userAndProducts){				 						
						itemId = r.product();
						
                        intRating.add(new Tuple2<>(itemId,r));	
					}
					
					return intRating;
				}
			});
			
			System.out.println("***********recommConversion2 "+recommConversion2.collect());	
			
			JavaPairRDD<Integer, Rating> itemBasedRating = ratings3.mapToPair(new PairFunction<Tuple2<Object,Rating[]>, Integer, Rating>() {

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
					return new Tuple2<Integer,Rating>(itemId, new Rating(userId, itemId, score));
				}
			});

			System.out.println("itemBasedRating "+ itemBasedRating.collect());					
		
			//Convert item id to item value in itemBaseRating			
	/*		JavaPairRDD<String, ALSRating> itemValueMap1  = itemBasedRating.join(itemHbaseMapLookupRddReverse).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Rating,String>>, String, ALSRating>() {
				@Override
				public Tuple2<String, ALSRating> call(Tuple2<Integer, Tuple2<Rating, String>> arg0) throws Exception {
					return new Tuple2<String,ALSRating>(arg0._2()._2, new ALSRating(String.valueOf(arg0._2()._1.user()), arg0._2()._2, arg0._2._1.rating()));
				}
			});	
			*/
			JavaPairRDD<String, ALSRating> itemValueMap  = recommConversion2.join(itemHbaseMapLookupRddReverse).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Rating,String>>, String, ALSRating>() {
				@Override
				public Tuple2<String, ALSRating> call(Tuple2<Integer, Tuple2<Rating, String>> arg0) throws Exception {
					return new Tuple2<String,ALSRating>(arg0._2()._2, new ALSRating(String.valueOf(arg0._2()._1.user()), arg0._2()._2, arg0._2._1.rating()));
				}
			});	
			
			
			System.out.println("itemValueMap "+ itemValueMap.collect());
			
			//Convert user id to user value
			JavaPairRDD<Integer, ALSRating> userBasedRating = itemValueMap.mapToPair(new PairFunction<Tuple2<String,ALSRating>, Integer, ALSRating>() {

				@Override
				public Tuple2<Integer, ALSRating> call(Tuple2<String, ALSRating> arg0) throws Exception {
					return new Tuple2<Integer,ALSRating>(Integer.valueOf(arg0._2().user), arg0._2);
				}
			});
			
			JavaPairRDD<String, ALSRating> userValueMap= userBasedRating.join(userMapLookupPairRddReverse).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<ALSRating,String>>, String, ALSRating>() {
				@Override
				public Tuple2<String, ALSRating> call(Tuple2<Integer, Tuple2<ALSRating, String>> arg0)throws Exception {
					return new Tuple2<String,ALSRating>(arg0._2()._2, new ALSRating(arg0._2._2(), arg0._2._1.product, arg0._2._1.rating));
				}
			});			
			
			System.out.println("userValueMap Count "+ userValueMap.count());	
			System.out.println("userValueMap "+ userValueMap.collect());					
								
			System.out.println("************UN-persisted*************");
			itemMapLookupPairRdd.unpersist();
						
			//5. Save userValueMap in Hbase
			//5.A Create JonConf
			//String scoreTable = "ProductRecommendationSession_07291114";
			String scoreTable = "ProductRecommendationSession_0809";
			
			config.set(TableOutputFormat.OUTPUT_TABLE, scoreTable);		
			
			Job newApiJobConfig = Job.getInstance(config);
			newApiJobConfig.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, scoreTable);
			newApiJobConfig.setOutputFormatClass(TableOutputFormat.class);
			newApiJobConfig.setMapOutputKeyClass(String.class);			
			
			//5.B Transform data for saving in Hbase.
			JavaPairRDD<ImmutableBytesWritable, Put> writeToHbase = userValueMap.mapToPair(new PairFunction<Tuple2<String,ALSRating>, ImmutableBytesWritable, Put>() {
				@Override
				public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, ALSRating> arg0) throws Exception {
					Put put = null;
					put = new Put(Bytes.toBytes(arg0._2().user));
					String value = arg0._2().product+";"+ arg0._2().rating;
					put.addColumn(Bytes.toBytes("recommendation"), Bytes.toBytes("product"),Bytes.toBytes(value));					
					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
				}
			});
						
				
			JavaPairRDD<String, Iterable<ALSRating>> writeToHbase2 = userValueMap.groupByKey();
			
			//Create Hbase table before Saving data
			HBaseAdmin admin = new HBaseAdmin(config);			
			if(!admin.isTableAvailable("ProductRecommendationSession_0809")) {
				HTableDescriptor tableDesc = new HTableDescriptor("ProductRecommendationSession_0809");
				tableDesc.addFamily(new HColumnDescriptor("recommendation"));
				admin.createTable(tableDesc);
			    }
					
			JavaPairRDD<ImmutableBytesWritable, Put> aa = writeToHbase2
					.mapToPair(new PairFunction<Tuple2<String, Iterable<ALSRating>>, ImmutableBytesWritable, Put>() {

						@Override
						public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Iterable<ALSRating>> arg0)
								throws Exception {
							Put put = null;
							String recommendation = "";
							Iterable<ALSRating> aa = arg0._2();
							
							for (ALSRating r : aa) {
								put = new Put(Bytes.toBytes(arg0._1));
								recommendation += r.product +";"+ r.rating + ";";	
								put = new Put(Bytes.toBytes(r.user));
							}
							put.addColumn(Bytes.toBytes("recommendation"), Bytes.toBytes("product"),Bytes.toBytes(recommendation));
							
							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
						}
					});
			
			/*System.out.println("writeToHbase2 Count "+ writeToHbase2.count());	
			System.out.println("writeToHbase2 "+ writeToHbase2.collect());*/
						
			//Write to Hbase
			aa.saveAsNewAPIHadoopDataset(newApiJobConfig.getConfiguration());
			System.out.println("Data saved to hbase");			
			
		} catch (Exception e) {
			e.printStackTrace();
			responseStr = e.getMessage();
		} finally {
			System.out.println("**********  finally ***************");
			responseStr = STARTOFSCORING + responseStr + ENDOFSCORING;
			System.out.println(responseStr);
		}
	}
}
