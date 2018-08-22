package webdriver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.skedgo.converter.TimezoneMapper;

public class Station {
	private String usaf 						= "";
	private String wban 						= "";
	private String stationName 					= "";
	private String country 						= "";
	private String state 						= "";
	private String icao							= "";
	private Double latitude						= 0.0;
	private Double longitude					= 0.0;
	private Double elevation					= 0.0;
	private static SimpleDateFormat dateFormat 	= new SimpleDateFormat("yyyyMMdd");
	private Date beginDate 						= null;
	private Date endDate 						= null;
	private String timeZone						= "";
	//private static Logger log 					= new Logger();
	
	public Station(String u, String w, String sn, String c, String st, String i, String la, String lo, String el, String bd, String ed) throws ParseException {
		usaf 			= u;
		wban 			= w;
		stationName 	= sn;
		country		 	= c;
		state 			= st;
		icao 			= i;
		latitude 		= la.length() == 0 ? 0.0 : Double.parseDouble(la);
		longitude 		= lo.length() == 0 ? 0.0 : Double.parseDouble(lo);
		elevation 		= el.length() == 0 ? 0.0 : Double.parseDouble(el);
		beginDate 		= dateFormat.parse(bd);
		endDate 		= dateFormat.parse(ed);
	}
	
	public String getUSAF() {
		return usaf;
	}
	public String getWBAN() {
		return wban;
	}
	public String getStationName() {
		return stationName;
	}
	public String getCountry() {
		return country;
	}
	public String getState() {
		return state;
	}
	public String getICAO() {
		return icao;
	}
	public Double getLatitude() {
		return latitude;
	}
	public Double getLongitude() {
		return longitude;
	}
	public Double getElevation() {
		return elevation;
	}
	public Date getBeginDate() {
		return beginDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public String getTimeZone() {
		return timeZone;
	}
	
	public void setTimeZone() {
		timeZone = TimezoneMapper.latLngToTimezoneString(getLatitude(), getLongitude());
		//log.message("	Time zone: "+timeZone+" from lat: "+getLatitude()+", lon: "+getLongitude()+", station: "+getWBAN()+", name: "+getStationName()+", state: "+getState());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((wban == null) ? 0 : wban.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Station)) {
			return false;
		}
		Station other = (Station) obj;
		if (wban == null) {
			if (other.wban != null) {
				return false;
			}
		} else if (!wban.equals(other.wban)) {
			return false;
		}
		return true;
	}

	
	
	/*
	 * equals - overrides the extended List class of ArrayList to check if the object has the WBAN value as 
	 * 			another already stored in the List
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	/*
	@Override
	public boolean equals(Object object) {
		boolean b = false;
		if (object != null && object instanceof Station) {
			b = this.wban == ((Station) object).wban;
		}
		return b;
	}
	*/
	
	
	
	
}