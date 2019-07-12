/**
 * 
 */
package RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

/**
 * The first thing a Spark program must do is to create a JavaSparkContext
 * object, which tells Spark how to access a cluster. To create a SparkContext
 * you first need to build a SparkConf object that contains information about
 * your application.
 *
 * setAppName() : The appName parameter is a name for your application to show
 * on the cluster UI setMaster() : Master is a Spark, Mesos or YARN cluster URL,
 * or a special “local” string to run in local mode
 */
public class BasicRDD {
	SparkConf conf = new SparkConf().setAppName("BasicRDD").setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);

	/* Parallelized collections are created by calling JavaSparkContext’s
	 * parallelize() method on an existing Collection in your driver program.
	 * The elements of the collection are copied to form a distributed
	 *  dataset that can be operated on in parallel.
	*/
	JavaRDD<Integer> parallizing() {		
		List<Integer> myList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(myList);		
		distData.foreach(i -> System.out.print(i+", "));
		return distData;
	}
	/*
	 * - Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
	 * - Text file RDDs can be created using SparkContext’s textFile method.
	 */
	JavaRDD<String> textfile() {
		JavaRDD<String> stringData = sc.textFile("C:\\Abhijeet\\Case2_TimeOut_sec_AfterDeleteWait.log");
		return stringData;
		//stringData.foreach(str -> System.out.println(str));
	}
	
	JavaRDD<Integer> basicTransformationAction(){
		JavaRDD<String> stringData = sc.textFile("C:\\Abhijeet\\Data.txt");
		//Example of transformation i.e. map(), which basically taking 
		// per String List <JavaRDD> and counts its length and save it to map
		JavaRDD<Integer> lineLengths = stringData.map(sD -> sD.length());
		lineLengths.foreach(myInt -> System.out.print(myInt+" ,"));
		// Above line prints the size of each list items, NOW if I want to print lineLength
		// again from calling function or outside this function, exactly what i did in Junit code
		// i returned the lineLength. It will not print the values, because SCOPE will lost.
		// To make the SCOPE present, we need to wite below Line StorageLevel.MEMORY_ONLY()
		// try to remove it and print it
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
		int totalLength = lineLengths.reduce((a,b)-> a+b);
		System.out.println("totalLength == "+totalLength);
		return lineLengths;
	}
}
