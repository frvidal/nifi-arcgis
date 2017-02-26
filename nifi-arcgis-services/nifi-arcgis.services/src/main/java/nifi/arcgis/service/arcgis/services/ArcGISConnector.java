package nifi.arcgis.service.arcgis.services;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.bouncycastle.crypto.modes.gcm.Tables1kGCMExponentiator;

import com.esri.arcgisruntime.data.FeatureType;
import com.esri.arcgisruntime.data.Field;
import com.esri.arcgisruntime.data.ServiceFeatureTable;
import com.esri.arcgisruntime.layers.FeatureLayer;
import com.esri.arcgisruntime.loadable.LoadStatus;

import nifi.arcgis.service.arcgis.services.json.ArcGISServicesData;
import nifi.arcgis.service.arcgis.services.json.Layer;

public class ArcGISConnector {

	final private String URL_REST_SERVICES = "/arcgis/rest/services/{0}?f=pjson";

	final private String URL_REST_LAYERS = "/arcgis/rest/services/{0}/FeatureServer?f=pjson";

	final private String URL_SERVICE_FEATURE_TABLE = "/arcgis/rest/services/{0}/MapServer/{1}";

	final private int ID_NOT_FOUND = -1;
	
	/**
	 * ArcGIS URL :
	 */
	private final String arcgisURL;

	/**
	 * folderServer : folder where the featureServers are located inside the root.
	 */
	private final String folderServer;

	/**
	 * featureServer : name of the FeatureServer
	 */
	private final String featureServer;

	/**
	 * table name
	 */
	private final String tableName;

	/**
	 * Apache NIFI logger
	 */
	private final ComponentLog logger;

	/**
	 * Main constructor
	 * 
	 * @param nifiLogger
	 *            : Main logger plugged into the NIFI framework 
	 * @param arcgisURL
	 *            : URL of the ArcGIS server
	 * @param folderServer
	 *            : Folder name starting from the root directory where the featureServers are located
	 * @param featureServer
	 *            : Name of the FeatureServer
	 * @param tableName
	 *            : Name of the table inside the ArcGIS database
	 */
	public ArcGISConnector(ComponentLog nifiLogger, 
			final String arcgisURL, 
			final String folderServer, 
			final String featureServer,
			final String tableName) {
		this.logger = nifiLogger;
		this.arcgisURL = arcgisURL;
		this.folderServer = folderServer;
		this.featureServer = featureServer;
		this.tableName = tableName;
	}

	/**
	 * This variable is used to verify that the checkConnection is finished
	 */
	volatile boolean checkConnectionFinished = false;

	/**
	 * Current working REST resource
	 */
	String currentRestResource;

	/**
	 * Current subject
	 */
	String currentSubject;

	public ValidationResult checkConnection() {

		final ValidationResult.Builder builder = new ValidationResult.Builder();

		try {

			currentRestResource = arcgisURL;
			currentSubject = "arcGIS URL";
			new URL(currentRestResource);

			ArcGISServicesData dataArcGIS = new ArcGISServicesData(logger);

			if ((folderServer != null) & folderServer.isEmpty()) {
				currentRestResource = arcgisURL + MessageFormat.format(URL_REST_SERVICES, "");
				currentSubject = "folder server";

				Set<String> registeredFolders = dataArcGIS.retrieveFolderServer(currentRestResource);
				if (!registeredFolders.contains(folderServer)) {
					throw new Exception("The folder " + folderServer + " does not exist on the server " + arcgisURL);
				}
				
			}

			currentRestResource = arcgisURL + MessageFormat.format(URL_REST_SERVICES, 
					((folderServer == null) || folderServer.isEmpty()) ? "" : folderServer );
			currentSubject = "featureLayer name";
			Set<String> registeredFeatureLayers = dataArcGIS.retrieveFeatureServer(currentRestResource);
			if (!registeredFeatureLayers.contains(featureServer)) {
				throw new Exception("The feature " + featureServer + " does not exist on the server " + arcgisURL);
			}
			
			
			currentRestResource = arcgisURL + MessageFormat.format(URL_REST_LAYERS, featureServer);
			currentSubject = "layer name";
			Set<Layer> registeredLayers = dataArcGIS.retrieveLayers(currentRestResource);
			int id = ID_NOT_FOUND;
			Optional<Layer> oLayer = registeredLayers.stream().filter(layer -> layer.name.equals(tableName)).findFirst();
			if (!oLayer.isPresent()) {
				throw new Exception("The layer " + tableName + " does not exist on the featureServer " + featureServer + " within the ArcGIS server " +  arcgisURL);				
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Found the layer ID " + oLayer.get().id + " for the layer name " + tableName);
			}
			
			currentRestResource = arcgisURL + MessageFormat.format(URL_SERVICE_FEATURE_TABLE, featureServer, oLayer.get().id);
			ServiceFeatureTable featureTable = new ServiceFeatureTable(currentRestResource);
			logger.info("Loading asynchronous the featureTable from " + currentRestResource + "...");
			featureTable.loadAsync();

			Runnable listener = () -> {
				LoadStatus ls = featureTable.getLoadStatus();
				if (ls.equals(LoadStatus.LOADED)) {

					if (logger.isDebugEnabled()) {
						logger.debug(featureTable.getTableName() + " hasGeometry() " + featureTable.hasGeometry());
						logger.debug("\tfeatureTable.canAdd() " + featureTable.canAdd());
						logger.debug("\tfeatureTable.getFeatureTemplates().isEmpty() "
								+ featureTable.getFeatureTemplates().isEmpty());
						logger.debug("\tfeatureTable.isEditable() " + featureTable.isEditable());

						logger.debug("Fields : ");
						List<Field> fields = featureTable.getFields();
						for (Field f : fields) {
							logger.debug("\t" + f.getName() + " " + f.getFieldType());
						}
					}
					builder.valid(true);
				}
				if (ls.equals(LoadStatus.FAILED_TO_LOAD) || ls.equals(LoadStatus.NOT_LOADED)) {
					logger.debug("URL Load failed ", featureTable.getLoadError());
					logger.debug("URL Load failed cause ", featureTable.getLoadError().getCause());
					String errorMessage = (featureTable.getLoadError().getCause() == null)
							? featureTable.getLoadError().getMessage().toString()
							: featureTable.getLoadError().getCause().toString();
					builder.subject("url ArcGIS & table name").input(arcgisURL).explanation(errorMessage).valid(false);
				}
				checkConnectionFinished = true;
			};

			featureTable.addDoneLoadingListener(listener);
		} catch (final Exception e) {
			logger.error("Rest resources unreachable " + currentRestResource);
			// ExceptionUtils.getStackTrace(e);
//			StringWriter sw = new StringWriter();
//			e.printStackTrace(new PrintWriter(sw));
			logger.error(ExceptionUtils.getStackTrace(e));
			return builder.input(currentRestResource).explanation(e.getMessage()).valid(false).subject(currentSubject)
					.build();

		}

		while (!checkConnectionFinished) {
			try {
				Thread.sleep(100, 0);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return builder.build();
	}
}
