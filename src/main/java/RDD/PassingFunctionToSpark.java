package RDD;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

class GetLength implements Function<String, Integer> {
	public Integer call(String s) {
		return s.length();
	}
}

class Sum implements Function2<Integer, Integer, Integer> {
	public Integer call(Integer a, Integer b) {
		return a + b;
	}
}

public class PassingFunctionToSpark implements Serializable {

	// Using Lambda Expression to call Functional Interface to submit your
	// definition in Spark

	public JavaRDD<Integer> PassingFunctionToSparkUsingLambda() {
		SparkConf conf = new SparkConf().setAppName("PassingFunctionToSpark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("C:\\Abhijeet\\Learning\\JavaSparkLearning\\Data.txt");

		// 1. Interface Function<T1,R>::call(T1 t){ .....; return R}

		/*
		 * JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		 * public Integer call(String s) { return s.length(); } });
		 */

		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
			public Integer call(String s) {
				return s.length();
			}
		});

		lineLengths.foreach(myData -> System.out.println(myData));
		lineLengths.persist(StorageLevel.MEMORY_ONLY());

		// 2. Interface Function2<T1,T2,R>::call(T1 t1, T2 t2){ .....; return R}
		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		return lineLengths;
	}
}
