package RDD;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;

import scala.Option;

class StringAccumulator extends AccumulatorV2<String, String>{

	@Override
	public void add(String v) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AccumulatorV2<String, String> copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void merge(AccumulatorV2<String, String> other) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String value() {
		// TODO Auto-generated method stub
		return null;
	}

		
}
public class AccumulatorDemo {
	
	public void myAccumulatorDemo() {
		SparkConf conf = new SparkConf().setAppName("AccumulatorDemo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> myList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(myList);	
		Accumulator<Double> myAccum = sc.doubleAccumulator(1, "Counter");
		
		distData.foreach(x->myAccum.add(2d));
		distData.foreach(i -> System.out.print(i+", "));
	}

}
