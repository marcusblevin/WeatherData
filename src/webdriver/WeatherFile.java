package webdriver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import noaa.ishJava;

public class WeatherFile {
	private String baseURL 			= "";
	private String filePath 		= "";
	private String stationFile		= "";
	private Station station 		= null;
	private String year 			= "";
	private List<HashMap<String,String>> wl	= new ArrayList<HashMap<String, String>>();
	private static Logger log 		= new Logger();
	
	public WeatherFile(String b, String f, Station s, String y) throws SocketException, IOException {
		year 		= y;
		baseURL 	= b+year+"/";  // go in to year sub-folder
		filePath 	= f;
		station		= s;
		stationFile = station.getUSAF()+"-"+station.getWBAN()+"-"+year+".gz";
		
		getFile();
		processFile(filePath+stationFile);
	}
	
	private void getFile() throws SocketException, IOException {
		new FTP(baseURL, filePath+stationFile, stationFile);
	}
	
	private void processFile(String stationFile) {
		File f 				= new File(stationFile);
		try {
			InputStream fs 		= new FileInputStream(f);
			InputStream gs 		= new GZIPInputStream(fs);
			Reader decoder 		= new InputStreamReader(gs, "UTF-8"); // assuming the character encoding
			BufferedReader br 	= new BufferedReader(decoder);
			
			ishJava ish					= new ishJava();
			String line					= "";
			HashMap<String, String> map = null;
			
			while ((line = br.readLine()) != null) {
				map = ish.parseLine(line);

				if (map != null) {
					wl.add(map);
				}
			}
			
			//log.message("Weather Records: "+wl.size());
			
			br.close();
			decoder.close();
			gs.close();
			fs.close();
		} catch (FileNotFoundException e) {
			log.message("File Not Found");
			e.printStackTrace();
		} catch (IOException e) {
			log.message("Couldn't Read GZIP file in");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// done with file, delete it
		if (!f.delete()) {
			log.message("	Failed to delete file: "+stationFile);
			f.deleteOnExit(); // register to be deleted on JVM exit
		}
	}
	
	public List<HashMap<String,String>> getWeatherList() {
		return wl;
	}
}