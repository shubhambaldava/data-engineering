package paypay.shubham.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import paypay.shubham.baldava.assignment.MainLogAnalyser;
import paypay.shubham.baldava.assignment.model.LogModel;
import scala.Tuple2;

public class Test {

	static JavaSparkContext sc;
	@BeforeClass
	public static void setup() {
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local")
				.set("spark.executor.memory", "1g")
				.set("spark.cores.max", "4");
		sc = new JavaSparkContext(conf);
	}

	
	// Test to check if our sessionization logic is correct
	@org.junit.Test
	public void testSessionizeMethod() throws URISyntaxException {
		List<Tuple2<String, Tuple2<Long, Iterable<LogModel>>>> allSessions = MainLogAnalyser.sessionizeFile(getTestLogRdd()).collect();
		assertTrue(allSessions.size() == 3);
		for(Tuple2<String, Tuple2<Long, Iterable<LogModel>>> session : allSessions) {
			if(session._1.equals("1.1.1.1")) {
				assertTrue(session._2._1 == 0 || session._2._1 == 960000);
				if(session._2._1 == 0) {
					for(LogModel log : session._2._2) {
						assertTrue(log.getUrl().equals("https://paytm.com:443/4"));
					}
				} else if(session._2._1 == 960000) {
					for(LogModel log : session._2._2) {
						assertTrue(log.getUrl().equals("https://paytm.com:443/1")
								|| log.getUrl().equals("https://paytm.com:443/2")
								|| log.getUrl().equals("https://paytm.com:443/3"));
					}
				} else {
					assertTrue(false);
				}
			} else if(session._1.equals("9.9.9.9")) {
				for(LogModel log : session._2._2) {
					assertTrue(log.getUrl().equals("https://paytm.com:443/5"));
				}
			} else {
				assertTrue(false);
			}
		}
	}
	
	// Test to check if our sessionization URL access info csv which contains ip, sessionStartTime, sessionEndTime, uniqueUrlHits, allHits
	@org.junit.Test
	public void testAccessResultCsv() throws URISyntaxException {
		List<String> allSessions = MainLogAnalyser.getSessionAccessInfo(MainLogAnalyser.sessionizeFile(getTestLogRdd())).collect();
		List<String> expectedOutput = Arrays.asList("1.1.1.1,2015-07-22T09:00:28.019143Z,2015-07-22T09:16:28.019143Z,\"https://paytm.com:443/1---1\nhttps://paytm.com:443/2---1\nhttps://paytm.com:443/3---1\",\"https://paytm.com:443/1\nhttps://paytm.com:443/2\nhttps://paytm.com:443/3\"",
				"1.1.1.1,2015-07-22T10:00:28.019143Z,2015-07-22T10:00:28.019143Z,\"https://paytm.com:443/4---1\",\"https://paytm.com:443/4\"",
				"9.9.9.9,2015-07-23T10:00:28.019143Z,2015-07-23T10:00:28.019143Z,\"https://paytm.com:443/5---1\",\"https://paytm.com:443/5\"");
		assertTrue(allSessions.size() == 3);
		assertTrue(allSessions.containsAll(expectedOutput));
	}
	
	// Test to check if our we are able to find highest session times among all IP
	@org.junit.Test
	public void testMaxSessionTime() throws URISyntaxException {
		List<Tuple2<Long, String>> sessionTimes = MainLogAnalyser.getLongestSessionIp(MainLogAnalyser.sessionizeFile(getTestLogRdd())).collect();
		sessionTimes.forEach(x -> System.out.println(x));
		
		assertTrue(sessionTimes.size() == 2);
		for(Tuple2<Long, String> entry : sessionTimes) {
			assertTrue((entry._1 == 960000 && entry._2.equals("1.1.1.1"))|| (entry._1 == 0 && entry._2.equals("9.9.9.9")));
		}
	}
	

	public JavaRDD<String> getTestLogRdd() throws URISyntaxException {
		URL res = MainLogAnalyser.class.getClassLoader().getResource("test.log");
		File file = Paths.get(res.toURI()).toFile();
		String absolutePath = file.getAbsolutePath();

		JavaRDD<String> logLines = sc.textFile(absolutePath);
		return logLines;
	}
	
	@AfterClass
	public static void teardDown() {
		sc.stop();
	}
}
