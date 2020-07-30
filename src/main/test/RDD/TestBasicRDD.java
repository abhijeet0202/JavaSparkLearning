/**
 * 
 */
package RDD;

import org.apache.spark.api.java.JavaRDD;

import junit.framework.TestCase;

/**
 * @author ezbanab
 *
 */
public class TestBasicRDD extends TestCase {
	BasicRDD obj = null;
	JavaRDD<Integer> myIntegerRDD;
	JavaRDD<String>  myStringRDD;
	PassingFunctionToSpark passingFunctionToSpark = null;
	KeyValuePairDemo keyValuePairDemo = null;
	AccumulatorDemo accumulatorDemo = null;
	
	/**
	 * Test method for {@link RDD.BasicRDD#parallizing()}.
	 */
	public void testParallizing() {
		try {
			obj = new BasicRDD();
			myIntegerRDD = obj.parallizing();
			obj = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testTextFile() {
		try {
			obj = new BasicRDD();
			myStringRDD =obj.textfile();
			obj = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testbasicTransformationAction() {
		try {
			obj = new BasicRDD();
			myIntegerRDD = obj.basicTransformationAction();
			System.out.println("======================================");
			myIntegerRDD.foreach(myInt -> System.out.print(myInt+" "));
			obj = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testPassingFunctionToSpark() {
		try {
			passingFunctionToSpark = new PassingFunctionToSpark();
			myIntegerRDD = passingFunctionToSpark.PassingFunctionToSparkUsingLambda();
			System.out.println("======================================");
			myIntegerRDD.foreach(myInt -> System.out.print(myInt+" "));
			passingFunctionToSpark = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testUseKeyValuePairing() {
		try {
			keyValuePairDemo = new KeyValuePairDemo();
			keyValuePairDemo.UseKeyValuePairing();
			keyValuePairDemo = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testmyAccumulatorDemo() {
		try {
			accumulatorDemo = new AccumulatorDemo();
			accumulatorDemo.myAccumulatorDemo();
			accumulatorDemo = null;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}

}
