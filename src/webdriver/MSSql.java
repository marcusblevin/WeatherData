package webdriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MSSql {
	public static String databaseName	  	= "";
    public static String connectionURL     	= "";
    public static String username          	= "";
    public static String password          	= "";
    public static Connection connection     = null;
    public static Statement statement       = null;
    public static Connection connectionTmp  = null;
    public static Statement statementTmp    = null;
    public static int batchinserts          = 10000;
    public SimpleDateFormat sdf				= new SimpleDateFormat("yyyyMMddHHmm");
    private static Logger log 				= new Logger();
	   
	public MSSql(String dbName, String conn, String user, String pwd, int bi) {
		databaseName	= dbName;
	    connectionURL 	= conn + dbName;
	    username       	= user;
	    password       	= pwd;
	    batchinserts   	= bi;
	    
	    getConnections();
	}
	
	private static void getConnections() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
			//Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(connectionURL, username, password);
			statement = connection.createStatement();
			connectionTmp = DriverManager.getConnection(connectionURL, username, password);
			statementTmp = connection.createStatement();
			log.message("DB Connection Open");
		} catch(Exception e) {
			e.printStackTrace();
			log.message("Exiting System");
			System.exit(1);
		}
    }
	
	public void closeConnections() throws SQLException {
		statement.close();
		connection.close();
		log.message("DB Connection Closed");
	}
	
	/*********************
	 * createNewWBAN .. step through stations to create tables and build insert prepared statement
	 * @param stationList 
	 * @throws SQLException
	 */
	public void createNewWBAN(ArrayList<Station> stationList) {
		createWeatherStations(); 
		
		try {
			log.message("Processing Station Updates / Creating Station Tables");
			String sqlStr = "IF EXISTS (SELECT * FROM ["+databaseName+"].[dbo].[stations] WITH (updlock,serializable) " +
		                     " WHERE station_id = ? AND station_type = 1) " +
		                     " UPDATE ["+databaseName+"].[dbo].[stations] " +
		                     " SET station_name  = ?, " +
		                     	" state = ?, " +
		                     	" latitude  = ?, " +
		                     	" longitude = ?," +
		                     	" elev = ?" +
		                     " WHERE station_id = ? AND " +
		                     	" station_type = 1 " +
		                     " ELSE INSERT INTO ["+databaseName+"].[dbo].[stations] " + 
		                     " (station_type, station_id, station_name, state, latitude, longitude, elev) VALUES " + 
		                     " ( 1, ?, ?, ?, ?, ?, ?);";
	
			connection.setAutoCommit(false);
			PreparedStatement ps 	= connection.prepareStatement(sqlStr);
			
			for (Station s: stationList) {
				createWBAN(s.getWBAN()); // create if not exist
				
				ps.setString(1, s.getWBAN());
				ps.setString(2, s.getStationName());
				ps.setString(3, s.getState());
				ps.setDouble(4, s.getLatitude());
				ps.setDouble(5, s.getLongitude());
				ps.setDouble(6, s.getElevation());
				ps.setString(7, s.getWBAN());
				ps.setString(8, s.getWBAN());
				ps.setString(9, s.getStationName());
				ps.setString(10, s.getState());
				ps.setDouble(11, s.getLatitude());
				ps.setDouble(12, s.getLongitude());
				ps.setDouble(13, s.getElevation());
				
				ps.addBatch();
			}
			
			ps.executeBatch();
			ps.clearBatch();
			connection.commit();
			connection.setAutoCommit(true);
			//System.out.println(" "); // move console cursor to new line
			log.message("	Processing "+stationList.size()+" stations");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/***********************
	 * createWeatherStations .. create stations table
	 * @throws SQLException
	 */
	private static void createWeatherStations() {
	    String sql = "IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[stations]')"
		               + " AND OBJECTPROPERTY(id, N'IsUserTable') = 1) "
		               + " CREATE TABLE [dbo].[stations] ("
			               + " station_type INT NOT NULL, "
			               + " station_id VARCHAR(120) NOT NULL, "
			               + " station_name VARCHAR(250), "
			               + " state VARCHAR(50), "
			               + " latitude DECIMAL(9,3), "
			               + " longitude DECIMAL(9,3), "
			               + " elev DECIMAL(9,3)"
		               + ");";
	    try {
	    	statement.execute(sql);
	    } catch (SQLException e) {
	    	e.printStackTrace();
	    }
	}
	
	 /**********************
      * createWBAN .. create one table
      * 			  Sets IGNORE_DUP_KEY option to ignore inserts on duplicate records
      * @param values
      * @throws SQLException 
      */
	 private static void createWBAN (String wban) {
		 String sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id(N'[dbo].[wban_"+wban+"]')"+
	                	" AND OBJECTPROPERTY(id, N'IsUserTable') = 1) "+
		                " CREATE TABLE ["+databaseName+"].[dbo].[wban_"+wban+"] ( " +
		                	" [date_timestamp] 	   DATETIME,"+
			                " [DryBulb::degF]      DECIMAL(9,3), " +
			                " [WetBulb::degF]      DECIMAL(9,3), " +
			                " [RelHumidity::pct]   DECIMAL(9,3), " +
			                " [Pressure::inHg]     DECIMAL(9,3), " +
			                " [Visibility::miles]  DECIMAL(9,3), " +
			                " [WindSpeed::knots]   DECIMAL(9,3), " +
			                " [WindDir::degreeN]   DECIMAL(9,3), " +
			                " [skycondition]	   VARCHAR(64), " +
			                " [HourlyPrecip] 	   DECIMAL(9,3) " +
		                     "CONSTRAINT PK_wban_"+wban+" PRIMARY KEY CLUSTERED(date_timestamp) WITH(IGNORE_DUP_KEY=ON) "+
		                ");";
		 try {
	    	  statement.execute(sql);
	    	  //System.out.print(wban + " ");
	      } catch (SQLException e) {
	    	  e.printStackTrace();
	      }
	 }
	 
	 
	 public static void dropTempIndex() throws SQLException {
		 String sql = "DROP INDEX PK_Temp_IDX ON tempdb.dbo.weather_temp";
		 try {
			 statement.execute(sql);
			 connection.commit();
		 } catch (SQLException e) {
			 throw (e);
		 }
	 }
	 
	 public static void createTempIndex() throws SQLException {
		 String sql = "CREATE UNIQUE CLUSTERED INDEX PK_Temp_IDX ON tempdb.dbo.weather_temp(wban_number,date_timestamp) WITH (FILLFACTOR = 100)";
		 try {
			 statement.execute(sql);
			 connection.commit();
		 } catch (SQLException e) {
			 throw (e);
		 }
	 }
	 
	 /*************
	  * createTempTable : creates a temp table that we will insert all values from file
	  * @throws SQLException 
	  */
	 public void createTempTable(String collation) throws SQLException {
		 // Need to worry about collations here since character comparisons might fail.
		 // create it in tempdb since this is not logged
	     String dropTable = " IF OBJECT_ID('tempdb.dbo.weather_temp','U') IS NOT NULL drop table tempdb.dbo.weather_temp";
	     String sql = "IF NOT EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE id = object_id(N'[tempdb].[dbo].[weather_temp]')" +
	                " AND OBJECTPROPERTY(id, N'IsUserTable') = 1) " +
	                " CREATE TABLE [tempdb].[dbo].[weather_temp] ( " +
	                " wban_number varchar(50) COLLATE " +  collation + " not null, " +
	                " [date_timestamp] 	   DATETIME, " +
	                " [DryBulb::degF]      DECIMAL(9,3), " +
	                " [WetBulb::degF]      DECIMAL(9,3), " +
	                " [RelHumidity::pct]   DECIMAL(9,3), " +
	                " [Pressure::inHg]     DECIMAL(9,3), " +
	                " [Visibility::miles]  DECIMAL(9,3), " +
	                " [WindSpeed::knots]   DECIMAL(9,3), " +
	                " [WindDir::degreeN]   DECIMAL(9,3), " +
	                " [skycondition]	   VARCHAR(64) COLLATE " + collation + "," +
	                " [HourlyPrecip] 	   DECIMAL(9,3) " +
	                ");";
	     // " CONSTRAINT PK_weather_temp PRIMARY KEY CLUSTERED (wban_number, date_timestamp) "+
	     try {
	    	 log.message("Dropping Temp Table");
	         statement.execute(dropTable);
	         System.out.println("    Creating Temp Table : ");
	         statement.execute(sql);
	         log.message("Created");
	     } catch (SQLException e) {
	         throw(e);
	     }
	 }
	 
	 public void dropTempTable() throws SQLException {
		 statement.execute("DROP TABLE [tempdb].[dbo].[weather_temp]"); //Drop Temporary tables and close
   	  	 connection.commit();
	 }
	 
	 /***********************
	  * addWBANBatch .. add weather file data to temp table to process in batches. Format dates and calculate wet bulb and humidity
	  * @param wl - hash map of weather file 
	  * @throws ParseException 
	  * @throws SQLException
	  */
	 public void addWBANBatch(List<HashMap<String, String>> wl, String timeZone) throws ParseException {
		 try {
			 String sql = "INSERT INTO [tempdb].[dbo].[weather_temp] ([wban_number], [date_timestamp], " +
	                  		" [DryBulb::degF], [WetBulb::degF], [RelHumidity::pct], [Pressure::inHg], [Visibility::miles], [WindSpeed::knots],"+
	                  		" [WindDir::degreeN],[SkyCondition], [HourlyPrecip]) VALUES " +
	                  		" (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
			 PreparedStatement ps 			= connectionTmp.prepareStatement(sql);
			 HashMap<String, String> map 	= null;
			 ArrayList<String> dateCheck	= new ArrayList<String>();
			 
			 for (int i=0; i<wl.size(); i++) {
				 map = wl.get(i);
				 
				 String dateRaw				= map.get("timestamp");
				 if (dateCheck.contains(dateRaw))  {
					 // date has already been processed and added to statement
					 // avoid duplicates by only inserting the first record processed
					 continue;
				 }
				 
				 dateCheck.add(dateRaw);
				 LocalDate datePart			= LocalDate.parse(dateRaw.substring(0, 8), DateTimeFormatter.ofPattern("uuuuMMdd"));
				 LocalTime timePart			= LocalTime.parse(dateRaw.substring(8), DateTimeFormatter.ofPattern("HHmm"));
				 OffsetDateTime date 		= OffsetDateTime.of(datePart, timePart, ZoneOffset.UTC);
				 //LocalDateTime localDate 	= date.toInstant().atZone(ZoneId.of(timeZone)).toLocalDateTime();
				 ZonedDateTime zonedDate 	= date.atZoneSameInstant(ZoneId.of(timeZone));
				 long millis 				= zonedDate.toInstant().toEpochMilli();
				 
				 //System.out.println(date+" to "+timeZone+": "+localDate);
				 //System.out.println(millis);
				 
				 //Date date 				= sdf.parse(map.get("timestamp")+"GMT");
				 Double dryBulb 		= toDouble(map.get("TEMP"));
				 Double wetBulb 		= null;
				 Double relHumidity 	= null;
				 Double pressure		= toDouble(map.get("STP")); // station pressure
				 Double visibility		= toDouble(map.get("VSB"));
				 Double windSpeed		= toDouble(map.get("SPD"));
				 Double windDir			= toDouble(map.get("DIR"));
				 String skyCondition 	= map.get("SKC");
				 Double hourlyPrecip 	= toDouble(map.get("PCP01"));
				 Double dewPoint		= toDouble(map.get("DEWP"));
				 
				 if (skyCondition.startsWith("*"))  {  skyCondition = null;  }
				 if (pressure == null)  {  pressure = toDouble(map.get("SLP"));  }  // sea level pressure
				 if (pressure != null)  {  pressure = mbarToHg(pressure);  } // convert pressure in millibars to mercury(Hg)
				 
				 if (dewPoint != null) {
					 relHumidity 	= calculateRelHumidity(dryBulb, dewPoint);
					 wetBulb 		= pressure == null ? calculateWetBulb(dryBulb, relHumidity) : calculateWetBulb(dryBulb, dewPoint, pressure);
				 }
				 //log.message("stations: "+map.get("WBAN")+", date: "+localDate+", temp: "+dryBulb+", pressure: "+pressure+", dewPoint: "+dewPoint+", relHumid: "+relHumidity+", wetBulb: "+wetBulb);
				 
				 ps.setString(1, map.get("WBAN"));
				 ps.setTimestamp(2, new Timestamp(millis)); // convert Date to SQL date and convert GMT time to local time zone
				 ps.setDouble(3, dryBulb);
				 if (wetBulb != null)  		{  ps.setDouble(4, wetBulb); 		}  else  {  ps.setNull(4, Types.DOUBLE);  }
				 if (relHumidity != null)	{  ps.setDouble(5, relHumidity);    }  else  {  ps.setNull(5, Types.DOUBLE);  }
				 if (pressure != null)  	{  ps.setDouble(6, pressure);  		}  else  {  ps.setNull(6, Types.DOUBLE);  }
				 if (visibility != null)	{  ps.setDouble(7, visibility);		}  else  {  ps.setNull(7, Types.DOUBLE);  }
				 if (windSpeed != null)		{  ps.setDouble(8, windSpeed);  	}  else  {  ps.setNull(8, Types.DOUBLE);  } 
				 if (windDir != null) 		{  ps.setDouble(9, windDir); 		}  else  {  ps.setNull(9, Types.DOUBLE);  }
				 if (skyCondition != null) 	{  ps.setString(10, skyCondition);  }  else  {  ps.setNull(10,Types.DOUBLE);  }
				 if (hourlyPrecip != null)	{  ps.setDouble(11, hourlyPrecip);  }  else  {  ps.setNull(11,Types.DOUBLE);  }
				 
				 ps.addBatch();
			 }
			 
			 ps.executeBatch();
			 connectionTmp.commit();
		 } catch (SQLException e) {
			 e.printStackTrace();
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	 }

	 /*
	  * toDouble - convert string to double if not set to "*" for blank and verify all digits
	  */
	 private static Double toDouble(String str) {
		 if (str.startsWith("*") || !str.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+"))  {  return null;  }
		 else  {  return Double.parseDouble(str);  }
	 }
	 
	 /*
	  * taken from: https://journals.ametsoc.org/doi/full/10.1175/JAMC-D-11-0143.1
	  * Calculates wet bulb temperature based on sea level pressure
	  */
	 
	 private Double calculateWetBulb(Double dryBulb, Double r) {
		 if (r == null) {
			 return null;
		 }
		 
		 Double t = toCelsius(dryBulb);
		 Double tw = t*Math.atan(0.151977*Math.pow((r+8.313659),0.5)) + Math.atan(t+r) - Math.atan(r - 1.676331) + 0.00391838*Math.pow(r,1.5) * Math.atan(0.023101*r) - 4.686035; 

		 return toFahrenheit(tw);
	 }
	 
	 /***********************
	  * function received from NOAA
	  * Computes wet bulb from temp, dew point and station pressure.
      *	and returns the temperature in Fahrenheit.
      *	@param temp a double containing the temperature (F)
      *	@param dp a double containing the Dew Point (F)
      *	@param sp a double containing the Station Pressure (In Hg)
      *	@return a double containing the  Wet Bulb (F)
	  */
	 private Double calculateWetBulb(Double temp, Double dp, Double sp) {
		 Double wb = 0.0;
		 Double A = (temp - dp) * .1;
		 Double B = A - 1.0;
		 Double C = Math.pow(A, 2);

	     if (temp >= 0) {
	    	 wb = temp - (.034 * A - .00072 * A * B) * (temp + dp - 2.0 * sp + 108.0);
	     } else {
	    	 wb = temp - (.034 * A - .006 * C) * (.6 * (temp + dp) - 2.0 * sp + 108.0);
	     }
	     return wb;
	 }

	 
	 /*
	  * equation taken from: http://andrew.rsmas.miami.edu/bmcnoldy/Humidity.html
	  */
	 private Double calculateRelHumidity(Double dryBulb, Double dewPoint) {
		 if (dewPoint == null)  {
			 return null;
		 }
		 
		 // convert to Celsius
		 Double t = toCelsius(dryBulb);
		 Double d = toCelsius(dewPoint);
		 
		 return 100 * (Math.exp((17.625*d)/(243.04+d)) / Math.exp((17.625*t)/(243.04+t)));
	 }
	 
	 private Double toCelsius(Double t) {
		 return ((0.5556)*(t-32.0));
	 }
	 
	 private Double toFahrenheit(Double t) {
		 return ((1.8*t)+32.0);
	 }
	 
	 /*
	  * convert millibars to inch mercury
	  * taken from: http://www.endmemo.com/sconvert/mbarinhg.php
	  */
	 private Double mbarToHg(Double mbar) {
		 return mbar * 0.02953;
	 }
	 
	 public void insertAll() throws SQLException {
		 try {
			 String sql = " SELECT DISTINCT wban_number FROM tempdb.dbo.weather_temp ";
	         createTempIndex(); // insertions finished create an index to speed this up
	         connection.commit();
	         List<String> wbans = new ArrayList<String>();
	         ResultSet rs = statement.executeQuery(sql);
	         while( rs.next() ){   
	        	 wbans.add(rs.getString(1));
	         }
	         rs.close();
	         System.out.print("    Inserting ");
	         for (String W : wbans){
	        	 insertWBan(W);
	         }
	         log.message(".");
	         dropTempIndex(); //drop the index to speed insertions up
	         connection.commit();
		 } catch (SQLException e) {
			 throw(e);
		 }
	 }
	 
	 /*******************
	  * insertWBan : inserts values from the temp table to wban_??? table
	  * @param values
	  * @throws SQLException 
	  */
	 public static void insertWBan(String values) throws SQLException {
		 String deleteString = " DELETE FROM tempdb.dbo.weather_temp WITH (TABLOCK) " +
	                           " WHERE EXISTS (" + 
	                             " SELECT * FROM wban_"+values+
	                             " WHERE tempdb.dbo.weather_temp.date_timestamp = wban_"+values+".date_timestamp AND " +
	                             		" tempdb.dbo.weather_temp.wban_number = '"+values+"') ";
	     String insertString = " INSERT wban_"+values + " WITH (TABLOCK) " +
	    		  " SELECT [date_timestamp],[DryBulb::degF],[WetBulb::degF],[RelHumidity::pct], [Pressure::inHg], [Visibility::miles],[WindSpeed::knots],"+
	    		  			"[WindDir::degreeN],[SkyCondition],[HourlyPrecip]" +
	    		  " FROM tempdb.dbo.weather_temp WHERE wban_number = '"+values+"' ";

	     try {
	    	 statement.addBatch(deleteString); // remove duplicates from temp table
	         statement.addBatch(insertString); // insert new values
	         statement.addBatch(deleteString); // remove inserted values
	         statement.executeBatch();
	         connection.commit();            // commit it one at a time
	         System.out.print(values+ " ");
	     } catch (SQLException e) {
	         throw(e);
	     }
	 }
	   
	 /* gets the sort order for comparisons == collation */
	 public String getCollation() throws SQLException {
		 String ret = null;
	     try {
	    	 ResultSet rs = statement.executeQuery("SELECT CONVERT( VARCHAR, DATABASEPROPERTYEX('"+databaseName+"','collation'))");
	    	 while (rs.next()) {
	    		 ret = rs.getString(1);
	    	 }
	    	 rs.close();
	     } catch (SQLException e) {
	    	 throw(e);
	     }
	     return ret;
	 }
}