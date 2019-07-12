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
	
	protected void setUp() throws Exception {
		super.setUp();
		obj = new BasicRDD();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		obj = null;
	}

	/**
	 * Test method for {@link RDD.BasicRDD#parallizing()}.
	 */
	public void testParallizing() {
		try {
			myIntegerRDD = obj.parallizing();
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testTextFile() {
		try {
			myStringRDD =obj.textfile();
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void testbasicTransformationAction() {
		try {
			myIntegerRDD = obj.basicTransformationAction();
			System.out.println("======================================");
			myIntegerRDD.foreach(myInt -> System.out.print(myInt+" "));
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}

}
