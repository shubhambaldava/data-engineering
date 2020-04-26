package paypay.shubham.baldava.assignment.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogModel implements Serializable, Comparable<LogModel> { 
	/**
	 * 
	 */
	private static final long serialVersionUID = -1891532587434740660L;

	@Override
	public String toString() {
		return "["+backendStatusCode + " " +elbStatusCode+ " "+ dateTimeString+"]";
	}


	private String clientIpAddress;

	private float elbStatusCode;
	private float backendStatusCode;

	private String dateTimeString;	

	private long time;
	private String url;

	//timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	private static final String LOG_ENTRY_PATTERN =
			//    "^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"])\" \"([^\"])\" (\\S+) (\\S+)$";
			//"^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"])\" \"([^\"])\" - -$";
			//"2016-02-16T01:00:50.309216Z ArabGT-Load-Balancer 105.198.242.106:15610 172.31.14.182:80 0.000042 0.000882 0.000023 304 304 0 0 \"GET http://www.arabgt.com:80/sites/default/files/js/js_pcG4nozC_WcLiZGYbq1zfRAv88ony8ZmiwgBXaRG1ik.js HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 9_2_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13D15 [FBAN/FBIOS;FBAV/48.0.0.64.162;FBBV/21316239;FBDV/iPhone8,1;FBMD/iPhone;FBSN/iPhone OS;FBSV/9.2.1;FBSS/2; FBCR/Vodafone;FBID/phone;FBLC/en_US;FBOP/5]\" - -";
			"^(\\S+) (\\S+) (\\S+):(\\S+) (\\S+):(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$";
	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public LogModel(
			String timestamp, 
			String elb,
			String clientIpAddress, 
			String clientPort, 
			String backendIpAddress, 
			String backendPort,
			String requestProcessingTime,
			String backendProcessingTime, 
			String responseProcessingTime,
			String elbStatusCode,
			String backendStatusCode,
			String receivedBytes,
			String sentBytes, 
			String request, 
			String userAgent,
			String sslCipher,
			String sslProtocol,
			long time,
			String url
			) {


		this.dateTimeString = timestamp;
		this.setClientIpAddress(clientIpAddress);
		this.elbStatusCode = Integer.parseInt( elbStatusCode );
		this.backendStatusCode = Integer.parseInt( backendStatusCode );
		this.setTime(time);
		this.setUrl(url);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public static LogModel parseFromLogLine(String logline) {
		Matcher m = PATTERN.matcher(logline);
		if (!m.find()) {
			//			System.out.println("Cannot parse logline:  " + logline);
			//			logger.log(Level.ALL, "Cannot parse logline  " + logline);
			return null;
		}
		long timeTemp = -1l;
		try {

			// GET TIMESTAMP FROM DATE
			DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;

			LocalDateTime ld = LocalDateTime.from(dtf.parse(m.group(1)));
			timeTemp = Timestamp.valueOf(ld).getTime();

		} catch (Exception e) {
			// TODO: handle exception
			return null;
		}
		if(timeTemp == -1l) {
			return null;
		}
		
		
		String url = "";
		try {

			// GET URL FROM REQUEST
			url = m.group(14).split(" ")[1];

		} catch (Exception e) {
			// TODO: handle exception
			return null;
		}
		if(url.isEmpty()) {
			return null;
		}



		return new LogModel(
				m.group(1),
				m.group(2), 
				m.group(3), 
				m.group(4),
				m.group(5),		
				m.group(6), 
				m.group(7),
				m.group(8), 
				m.group(9),
				m.group(10),
				m.group(11),
				m.group(12),
				m.group(13),
				m.group(14),
				m.group(15),
				m.group(16),
				m.group(17),
				timeTemp,
				url
				);
	}

	

	public String getDateTimeString() {
		return dateTimeString;
	}

	public void setDateTimeString(String dateTimeString) {
		this.dateTimeString = dateTimeString;
	}

	public String getClientIpAddress() {
		return clientIpAddress;
	}

	public void setClientIpAddress(String clientIpAddress) {
		this.clientIpAddress = clientIpAddress;
	}


	public float getElbStatusCode() {
		return elbStatusCode;
	}

	public void setElbStatusCode(float elbStatusCode) {
		this.elbStatusCode = elbStatusCode;
	}

	public float getBackendStatusCode() {
		return backendStatusCode;
	}

	public void setBackendStatusCode(float backendStatusCode) {
		this.backendStatusCode = backendStatusCode;
	}

	public static String getLogEntryPattern() {
		return LOG_ENTRY_PATTERN;
	}

	public static Pattern getPattern() {
		return PATTERN;
	}

	@Override
	public int compareTo(LogModel o) {
		return Long.compare(this.time, o.getTime());
	}




}