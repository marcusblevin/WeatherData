package webdriver;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

public class runImport {
	public static Properties p = new Properties();
	   
    public static void main(String[] args) {
		try {
			String path = System.getProperty("user.dir");
			p.load(new FileInputStream(path+"/weather.properties"));			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	String filepath         = p.getProperty("weatherdata.folder");
        String connectionURL    = p.getProperty("connection.URL").trim();		
        String username         = p.getProperty("user.name").trim();
        String password         = p.getProperty("password").trim();
        int batchInserts        = Integer.valueOf(p.getProperty("batchinserts"));
        String baseUrl			= p.getProperty("weatherUrl").trim();
        String databaseName		= p.getProperty("databaseName").trim();
        
        // Overriders here if you want to do it manually
        databaseName			= "Weather_Test";
        filepath				= "C:/Users/mlevin/Documents/GitHub/WeatherData/WeatherFiles/";
        //webroot				= "c:/Users/mlevin/Documents/Projects/WeatherData4.0/";
        connectionURL     		= "jdbc:sqlserver://thor.graphet.local;Instance=THOR;DatabaseName=";
 	    username             = "jdbcaccess";
 	    password             = "jdbc";
        
        /*
         * 1. FTP to NOAA and download isd-history.csv - file includes all stations and data ranges for each
         * 2. Load CSV into ArrayList
         * 3. Filter down list to only include lines with WBAN numbers and with END dates greater than 2018
         * 4. Get list of years of END dates from file (currently 2018, potentially more as years progress)
         * 5. Insert any missing station IDs into stations table in database
         * 6. Step through stations, calculate time zone for station based on latitude/longitude 
         * 7. insert any new stations and update existing stations
         * 8. for each station, download gzip files from FTP
         * 9. Unzip file in memory
         * 10. Step through file and use ishJava to convert weather file to ArrayList
         * 11. convert GMT timestamp to local time
         * 12. calculate wet bulb and relative humidity
         * 13. Add values to batches and process
         * 14. Delete all files and cleanup
         * 
         */
        
        StationList sl 					= new StationList(baseUrl, filepath);        
        MSSql sql						= new MSSql(databaseName, connectionURL, username, password, batchInserts);
        ArrayList<Station> stationList 	= sl.getStationList();
        ArrayList<String> yearList		= sl.getYearList();
        
        try {
	        String collation = sql.getCollation();   //Find database collation 
	  	  	sql.createTempTable(collation);          //tempdb tables must be created with correct collation
	        sql.createNewWBAN(stationList);
	        //int count = 0;
        
	        // step through each station
        	for (Station s: stationList) {
		        // step through each year
		        for (String y: yearList) {
		        	WeatherFile wf = new WeatherFile(baseUrl, filepath, s, y);
		        	sql.addWBANBatch(wf.getWeatherList(), s.getTimeZone());
	        	}
		        
		        //if (count > 60)  {  break;  }
		        //count++;
	        }
        	
        	sql.insertAll();
	        sql.dropTempTable();
        
	        // done processing everything, close connection
        	sql.closeConnections();
        } catch (Exception e) {
        	e.printStackTrace();
        }

    }

}
