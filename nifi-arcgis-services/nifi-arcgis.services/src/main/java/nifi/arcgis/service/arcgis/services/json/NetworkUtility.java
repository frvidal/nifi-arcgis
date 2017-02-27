package nifi.arcgis.service.arcgis.services.json;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;

public class NetworkUtility {
	
	public static boolean isReachable(String targetUrl) throws IOException
	{
	    HttpURLConnection httpUrlConnection = (HttpURLConnection) new URL(
	            targetUrl).openConnection();
	    httpUrlConnection.setRequestMethod("HEAD");

	    try
	    {
	        int responseCode = httpUrlConnection.getResponseCode();

	        return responseCode == HttpURLConnection.HTTP_OK;
	    } catch (UnknownHostException noInternetConnection)
	    {
	        return false;
	    }
	}
}
