/**
 * 
 */
package nifi.arcgis.service.arcgis.services.json;

import static  nifi.arcgis.service.arcgis.services.json.NetworkUtility.isReachable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.logging.ComponentLog;

import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;

/**
 * Class in charge of retrieving information form the AcrGIS server
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class ArcGISServicesData {

	/**
	 * Apache NIFI logger
	 */
	private final ComponentLog logger;

	public ArcGISServicesData (ComponentLog logger) {
		this.logger = logger;
	}
	
	private String restCall(final String address) throws Exception {

		logger.debug("Calling " + address);
		
		final StringBuilder sb = new StringBuilder();
		try {

			URL url = new URL(address);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

			String output;
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			conn.disconnect();

		} catch (MalformedURLException e) {
			logger.error(Arrays.toString(e.getStackTrace()));
			throw e;
		} catch (IOException e) {
			logger.error(Arrays.toString(e.getStackTrace()));
			throw e;
		} 
		return sb.toString();			
	}

	/**
	 * Get the folders available on the root directory.
	 * @param address the REST URL address
	 * @return The list of folders available on this server
	 * @exception Exception occurs during the network invocation
	 */
	public Set<String> retrieveFolderServer(final String address) throws Exception {
		return getFolderServer(restCall(address));
	}

	/**
	 * Extract the folders from a JSON response.
	 * @param json JSON String
	 * @return set of featureServers available
	 */
	public Set<String> getFolderServer(final String json) {
		logger.debug("reading JSON " + json);
		JSONArray folders = JsonPath.parse(json).read("$.folders.*", JSONArray.class);
		Set<String> folderServers = new HashSet<String>();
		for (int i = 0; i < folders.size(); i++) {
				folderServers.add((String) JsonPath.parse(folders.get(i)).read("$"));
		}
		return folderServers;
	}

	/**
	 * Get the feature Server from an URL.
	 * @param address the REST URL address
	 * @return The list of featureSevers available on this server
	 * @exception Exception occurs during the network invocation
	 */
	public Set<String> retrieveFeatureServer(final String address) throws Exception {

		if (!isReachable(address)) {
			throw new Exception(address + " is actually unrechable!");
		}
		
		return getFeatureServer(restCall(address));
	}

	/**
	 * Extract the feature Server from a JSON response.
	 * @param json JSON String
	 * @return set of featureServers available
	 */
	public Set<String> getFeatureServer(final String json) {
		logger.debug("reading JSON : " + json);
		JSONArray services = JsonPath.parse(json).read("$.services.*", JSONArray.class);
		Set<String> featureServers = new HashSet<String>();
		for (int i = 0; i < services.size(); i++) {
			if ( ((String) JsonPath.parse(services.get(i)).read("$.type")).equals("FeatureServer")) {
					featureServers.add((String) JsonPath.parse(services.get(i)).read("$.name"));
			}
		}
		return featureServers;
	}
	
	/**
	 * Get the layers available on the featureServer REST exploration URL
	 * @param address the REST exploration URL address
	 * @return The list of layers available on this server
	 * @exception Exception occurs during the network invocation
	 */
	public Set<Layer> retrieveLayers(final String address) throws Exception {
		return getLayers(restCall(address));
	}
	
	/**
	 * Extract the layers from a JSON response.
	 * @param json JSON String
	 * @return set of layers
	 */
	public Set<Layer> getLayers(final String json) {
		logger.debug("reading JSON " + json);
		JSONArray layersArray = JsonPath.parse(json).read("$.layers.*", JSONArray.class);
		Set<Layer> layers = new HashSet<Layer>();
		for (int i = 0; i < layersArray.size(); i++) {
			logger.debug(layersArray.get(i).toString());
			final int id = JsonPath.parse(layersArray.get(i)).read("$.id");
			final String name = (String) JsonPath.parse(layersArray.get(i)).read("$.name");
			layers.add(new Layer(id, name));
		}
		return layers;
	}
	
}
