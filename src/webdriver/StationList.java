package webdriver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Scanner;

public class StationList {
	
	public static String baseUrl 					= "";
	public static String filePath 					= "";
	public static String stationFile				= "isd-history.csv";
	public static ArrayList<Station> stationList 	= new ArrayList<Station>();
	public static SimpleDateFormat dateFormat 		= new SimpleDateFormat("yyyy-MM-dd");
	public static ArrayList<String> yearList		= new ArrayList<String>();
	private static Logger log 						= new Logger();
	
	public StationList(String bUrl, String fPath) {
		baseUrl = bUrl;
		filePath = fPath;
		
		try {
			log.message("fetching stationfile");
			getStationFile();
			log.message("processing station file");
			processStationFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * get station file to list all available stations and range available
	 */
	public void getStationFile() throws SocketException, IOException {
		new FTP(baseUrl, filePath+stationFile, stationFile);
	}
	
	/*
	 * read in station file and convert to ArrayList of station object
	 */
	public void processStationFile() throws FileNotFoundException, ParseException {
		File f 			= new File(filePath+stationFile);
		Scanner scan 	= new Scanner(f);
		int skipped = 0;
		scan.nextLine(); // first line is column headers, advance past
		while (scan.hasNextLine()) {
			String line 		= scan.nextLine();
			line				= line.replaceAll("\"", ""); // remove quotes from line
			String[] lineArr 	= line.split(",");
			
			if (!lineArr[1].isEmpty() && !lineArr[1].equals("99999") && lineArr[3].equals("US")) { // make sure WBAN is not blank or invalid and only US stations
				
				Station s = new Station(lineArr[0], lineArr[1], lineArr[2], lineArr[3], lineArr[4], lineArr[5], lineArr[6], lineArr[7], lineArr[8], lineArr[9], lineArr[10]);
				
				// only include stations with data available past 2018 and have not been added already
				// station history file can have duplicate WBAN stations for USAF base and non-USAF base
				if (s.getEndDate().after(dateFormat.parse("2018-01-01")) && !stationList.contains(s)) { 
					String year = lineArr[10].substring(0, 4);
					s.setTimeZone(); // station is valid, find time zone
					if (!yearList.contains(year))  {  yearList.add(year);  } // track the years we will need to download
					stationList.add(s);
				} else {
					skipped++;
				}
			} else {
				skipped++;
			}
		}
		log.message("	skipped "+skipped+" stations (non WBAN / old data / non-US)");
		log.message("	total stations: "+stationList.size());
		log.message("	years found: "+yearList.size());
		scan.close();

		
		if (!f.delete()) {
			log.message("	Failed to delete station file");
			f.deleteOnExit(); // register to be deleted on JVM exit
		}
	}
	
	public ArrayList<Station> getStationList() {
		return stationList;
	}
	
	public ArrayList<String> getYearList() {
		return yearList;
	}
}