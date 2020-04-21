package cs523.SparkWC;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser implements Serializable {
	
	private static final long serialVersionUID = -8083492734059418902L;
	public static final int NUM_FIELDS = 7;
	public static final String logEntryPattern = "^([\\d.]+|[\\d|\\D]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-)";
	
	private String ipAddress;
	private String responseCode;
	public String getIpAddress() {
		return ipAddress;
	}
	public String getResponseCode() {
		return responseCode;
	}
	public Date getDateTime() {
		return dateTime;
	}
	private Date dateTime;
	
	public LogParser(){
		ipAddress = "";
		responseCode = "";
		dateTime = new Date();
	}
	
	public LogParser(String _ipAddress, Date _dateTime, String _responseCode){
		ipAddress = _ipAddress;
		responseCode = _responseCode;
		dateTime = _dateTime;
	}
	
	public LogParser parseLog(String line){


    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher = p.matcher(line);
    if (!matcher.matches() || 
      NUM_FIELDS != matcher.groupCount()) {
      System.err.println("Bad log entry (or problem with RE?):");
    }
    
    Date date = new Date();
	try {
		date = new SimpleDateFormat("dd/MMM/yyyy").parse(matcher.group(4));
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}  
	
	String ip = matcher.group(1);
	String response = matcher.group(6);
    
    return new LogParser(ip, date, response);
}

}