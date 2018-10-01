# WeatherData

This program connects to the FTP server for [NOAA](https://www.ncdc.noaa.gov/) and downloads weather data files to parse and save to MSSQL database.

Process is as follows:

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

## Getting Started

You will need to have a Microsoft SQL Server database set up and configured for remote connection prior to running. If you would like to port over to MySQL, connection drivers and statements will need to be amended.

### Prerequisites

Weather Data timestamps from NOAA are saved in GMT but we need them to be in the local timezone of the client. This requires using [TimezoneMapper](https://github.com/drtimcooper/LatLongToTimezone) to get the timezone based on the longitude/latitude of the client.

In order for daylight savings to be handled correctly, the [Timezone Updater Tool (tzUpdater)](https://www.oracle.com/technetwork/java/javase/documentation/tzupdater-readme-136440.html) should be run to update the JDK and JRE with more recent timezone data.

[Microsoft JDBC driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/using-the-jdbc-driver?view=sql-server-2017) is required to connect to a database.

[Apache Commons Net 3.6](https://commons.apache.org/proper/commons-net/) is required for FTP connections

NOAA provided ishJava.java file was modified from parsing a zip archive into a readable format to returning a HashMap of each line. Original ishJava.java file can be retrieved from NOAA's FTP site.

## Running

The configuration file `weather.properties` should be configured with appropriate values for your organization.

The database will need to be created for connection purposes but all tables will be created on run as necessary.

As files are processed, they will be deleted to minimize footprint. Insert statements will be processed in bulk to speed up transactions.