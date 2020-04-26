package paypay.shubham.baldava.assignment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import paypay.shubham.baldava.assignment.model.LogModel;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

public class MainLogAnalyser {




	@SuppressWarnings("resource")
	public static void main(String[] args) throws URISyntaxException, IOException {
		
		unGunzipFile(new File(".").getCanonicalPath()+"/data/2015_07_22_mktplace_shop_web_log_sample.log.gz", new File(".").getCanonicalPath()+"/data/log");
		
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local")
				.set("spark.executor.memory", "1g")
				.set("spark.cores.max", "4");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		File logFile = new File(new File(".").getCanonicalPath()+"/data/log");
		
		JavaRDD<String> logLines = sc.textFile(logFile.getAbsolutePath());


		// STEP 1 - Sessionize

		// IP - <timeOfSession, list of logs in that>
		// Can have multiple pairs with same IP if multiple sessions
		JavaPairRDD<String, Tuple2<Long, Iterable<LogModel>> > sessions = sessionizeFile(logLines);



		// STEP 2 - Get ip-session-time-url info
		// This will generate a csv with following fields 
		// IP, sessionStartTime, sessionEndTime, uniqueUrlWithCount, allUrlHitsInSession
		// part-0000 will contain csv info

		getSessionAccessInfo(sessions)
		.coalesce(1, true)
		.saveAsTextFile(new File(".").getCanonicalPath()+"/sessionUrlInfo");


		// STEP 3 - Get longest sessions info

		// This will generate a csv with following fields 
		// totalSessionTime, IP
		// part-0000 will contain csv info
		getLongestSessionIp(sessions)
		.coalesce(1, true)
		.saveAsTextFile(new File(".").getCanonicalPath()+"/longestSessionIp");


		// Stop the Spark Context before exiting.
		sc.stop();

	}

	public static JavaPairRDD<String, Tuple2<Long, Iterable<LogModel>> >  sessionizeFile(JavaRDD<String> logLines) {
		return logLines
				// Parse all the logs into POJO class
				.map(LogModel::parseFromLogLine)

				//Only filter status codes 200.
				.filter(x -> x!= null && x.getElbStatusCode() == 200.0f && x.getBackendStatusCode() == 200.0)

				// Create different sessions if the difference between first and last access times diff by 15 mins
				.mapToPair(x -> new Tuple2<String, LogModel>(x.getClientIpAddress(), x))
				.groupByKey()
				.flatMapToPair(bucketingAllAccesses).cache();
	}


	public static JavaRDD<String> getSessionAccessInfo(JavaPairRDD<String, Tuple2<Long, Iterable<LogModel>> > sessions) {
		return sessions
				.map(convertSessionsToCsv);
	}

	public static JavaPairRDD<Long, String> getLongestSessionIp(JavaPairRDD<String, Tuple2<Long, Iterable<LogModel>> > sessions) {
		return sessions
				.groupByKey()
				.mapToPair(new PairFunction<Tuple2<String,Iterable<Tuple2<Long,Iterable<LogModel>>>>, Long, String>() {

					private static final long serialVersionUID = -6386926653477557385L;

					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Tuple2<Long, Iterable<LogModel>>>> t) throws Exception {
						long totalSessionTime = 0;
						for(Tuple2<Long, Iterable<LogModel>> session : t._2) {
							totalSessionTime += session._1;
						}
						return new Tuple2<Long, String>(totalSessionTime, t._1);
					}
				})
				.sortByKey(false);
	}




	//------------------ Function to Sessionize accesses -------------------------
	// INPUT : This function will get all accesses done by user throughout the log
	// OUTPUT : Iterator<Ip-Session> 
	static PairFlatMapFunction<Tuple2<String, Iterable<LogModel>>, String, Tuple2<Long, Iterable<LogModel>>> bucketingAllAccesses = 
			new PairFlatMapFunction<Tuple2<String, Iterable<LogModel>>, String, Tuple2<Long, Iterable<LogModel>>>() {

		private static final long serialVersionUID = 6520729072269731381L;

		@Override
		public Iterator<Tuple2<String, Tuple2<Long, Iterable<LogModel>>>> call(
				Tuple2<String, Iterable<LogModel>> allUserAccesses) throws Exception {
			List<Tuple2<String, Tuple2<Long, Iterable<LogModel>> >> listOfSessions = new ArrayList<Tuple2<String, Tuple2<Long, Iterable<LogModel>>>>();

			// Get & Sort all the access timestamps
			List<Long> sortedTime = new ArrayList<Long>();
			for(LogModel l : allUserAccesses._2) {
				sortedTime.add(l.getTime());
			}
			Collections.sort(sortedTime);


			// Find which session bucket each timestamp belongs to
			Map<Long, Integer> timeStampToBucket = new HashMap<Long, Integer>();
			Map<Integer, List<LogModel>> bucketToLogs = new HashMap<Integer, List<LogModel>>();
			// Stores sessionBucket to sessionTime
			Map<Integer, Long> bucketToSessionTime = new HashMap<Integer, Long>();


			long lastAddedTime = sortedTime.get(0);
			long firstTimestampOfBucket = sortedTime.get(0);
			int bucketId = 0;
			timeStampToBucket.put(lastAddedTime, bucketId);
			bucketToLogs.put(0,  new LinkedList<LogModel>());
			for(long tempTime : sortedTime) {
				if(tempTime - lastAddedTime > 900000 ) { // We only create a new bucket if the timestamps are 15 mins aparts
					bucketToSessionTime.put(bucketId, lastAddedTime-firstTimestampOfBucket);
					firstTimestampOfBucket = tempTime;
					bucketId++;
					bucketToLogs.put(bucketId,  new LinkedList<LogModel>());
				}
				timeStampToBucket.put(tempTime, bucketId);
				lastAddedTime = tempTime;
			}
			bucketToSessionTime.put(bucketId, lastAddedTime-firstTimestampOfBucket);


			// Create real Session lists of all the log data according the buckets
			for(LogModel log : allUserAccesses._2) {
				bucketToLogs.get(timeStampToBucket.get(log.getTime())).add(log);
			}
			for(Map.Entry<Integer, List<LogModel>> session : bucketToLogs.entrySet()) {
				listOfSessions.add(new Tuple2<String, Tuple2<Long, Iterable<LogModel>>>(allUserAccesses._1, 
						new Tuple2<Long, Iterable<LogModel>>(bucketToSessionTime.get(session.getKey()), session.getValue())));
			}

			// return iterator of this list of lists
			return listOfSessions.iterator();
		}
	};


	//------------------ Function to Generate String RDD which helps in CSV creation for sessions accesse info -------------------------
	// Function to convert sessionsRdd into required CSV strings RDD
	static Function<Tuple2<String,Tuple2<Long, Iterable<LogModel>>>, String> convertSessionsToCsv = new Function<Tuple2<String,Tuple2<Long, Iterable<LogModel>>>, String>() {

		private static final long serialVersionUID = -1448455399977158615L;

		@Override
		public String call(Tuple2<String, Tuple2<Long, Iterable<LogModel>>> v1) throws Exception {

			StringBuilder result = new StringBuilder(v1._1+",");
			StringBuilder uniqueUrlAndCount = new StringBuilder("\"");
			StringBuilder allUrlHits = new StringBuilder("\"");
			String sessionStartTime = "", sessionEndTime = "";
			long sessionStartTimestamp = -1, sessionEndTimestamp = -1;
			Map<String, Integer> urlMap = new HashMap<String, Integer>();
			for(LogModel l : v1._2._2) {
				if(sessionStartTime.isEmpty()) {
					sessionStartTime = l.getDateTimeString();
					sessionEndTime = l.getDateTimeString();
					sessionStartTimestamp = l.getTime();
					sessionEndTimestamp = l.getTime();
				}
				// Comparing longs to save time, space
				if(sessionEndTimestamp < l.getTime()) {
					sessionEndTime = l.getDateTimeString();
				}
				if(sessionStartTimestamp > l.getTime()) {
					sessionStartTime = l.getDateTimeString();
				}
				if(urlMap.containsKey(l.getUrl())) {
					urlMap.put(l.getUrl(), urlMap.get(l.getUrl())+1);
				} else {
					urlMap.put(l.getUrl(), 1);
				}
				allUrlHits.append(l.getUrl()+"\n");
			}
			for(Entry<String, Integer> e : urlMap.entrySet()) {
				uniqueUrlAndCount.append(e.getKey()+"---"+e.getValue()+"\n");
			}
			allUrlHits.deleteCharAt(allUrlHits.length()-1);
			allUrlHits.append("\"");
			uniqueUrlAndCount.deleteCharAt(uniqueUrlAndCount.length()-1);
			uniqueUrlAndCount.append("\",");

			result.append(sessionStartTime+",");
			result.append(sessionEndTime+",");
			result.append(uniqueUrlAndCount);
			result.append(allUrlHits);
			return result.toString();
		}
	};
	
	
	public static void unGunzipFile(String compressedFile, String decompressedFile) {
		 
        byte[] buffer = new byte[1024];
 
        try {
 
            FileInputStream fileIn = new FileInputStream(compressedFile);
 
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
 
            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
 
            int bytes_read;
 
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
 
                fileOutputStream.write(buffer, 0, bytes_read);
            }
 
            gZIPInputStream.close();
            fileOutputStream.close();
 
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}