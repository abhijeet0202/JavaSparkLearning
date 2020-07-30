package RDD;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

public class KeyValuePairDemo {

	public void UseKeyValuePairing() {
		SparkConf conf = new SparkConf().setAppName("KeyValuePairDemo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Abhijeet\\Learning\\JavaSparkLearning\\Data.txt");
		/*
		 * Map<String, Integer> myMap = new HashMap<String, Integer>(); myMap.put("a",
		 * 1); myMap.put("b", 1); myMap.put("a", 1); myMap.put("a", 1); myMap.put("b",
		 * 1); myMap.put("b", 1); myMap.put("b", 1); myMap.put("b", 1);
		 * JavaPairRDD<String, Integer> x = sc.parallelize(myMap);
		 */
		
		JavaPairRDD<String, Integer> pair = lines.mapToPair(s -> new Tuple2(s, 1));
		pair.collect().forEach(s->System.out.println(s._1+"----"+s._2));
		
		JavaPairRDD<String, Integer> countKey = pair.reduceByKey((a,b)-> a+b );
		countKey.collect().forEach(s->System.out.println(s._1+"---"+s._2));
		
		JavaPairRDD<String, Integer> countSorting = countKey.sortByKey(true);
		countSorting.collect().forEach(s->System.out.println(s._1+"--"+s._2));

	}
}
