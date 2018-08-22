package webdriver;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Logger {
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	/* 
	 * message : just blurt it out with timestamp
	 */
	public void message(String str) {
		Calendar cal = Calendar.getInstance();
	 	System.out.println(str + " -- " + dateFormat.format(cal.getTime()));  
	}
}