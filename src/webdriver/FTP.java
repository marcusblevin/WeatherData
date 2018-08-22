package webdriver;



import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketException;
import org.apache.commons.net.ftp.FTPClient;
//import org.apache.commons.net.ftp.FTPFile;

public class FTP {
	public static String hostname 		= "";
	public static String localFilePath	= "";
	public static String remoteFilename	= "";
	private static Logger log 			= new Logger();
	
	public FTP(String host, String local, String remote) throws SocketException, IOException {
		localFilePath 	= local;
		remoteFilename 	= remote;
		String dir 		= "";
		
		FTPClient ftp = new FTPClient();
		
		try {
			// split the connection string by the first "/" to get the host domain name
			// and the sub directory needed to navigate to
			String[] stSplit = host.split("/",2);
			hostname = stSplit[0];
			if (stSplit.length > 1)  {  dir = stSplit[1];  }
			
			ftp.setConnectTimeout(60000); // 60 seconds timeout
			ftp.connect(hostname);
			ftp.enterLocalPassiveMode();
			ftp.login("anonymous", ""); // required even though login not required
		
			// if a sub-directory is present, navigate to it
			if (!dir.isEmpty())  {  ftp.changeWorkingDirectory(dir);  }
			// list the files of the current directory
			/*
			FTPFile[] files = ftp.listFiles();  
			System.out.println("Listed "+files.length+" files.");
			for(FTPFile file : files) {
				System.out.println(file.getName());
			}
			*/
		    // lets pretend there is a JPEG image in the present folder that we want to copy to the desktop (on a windows machine)
			ftp.setFileType(FTPClient.BINARY_FILE_TYPE); // don't forget to change to binary mode! or you will have a scrambled image!
		    FileOutputStream br = new FileOutputStream(localFilePath);
		
		    ftp.retrieveFile(remoteFilename, br);
		    br.close();
		    
		    log.message("	received file: "+remoteFilename);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ftp.disconnect();
		}
	}
}