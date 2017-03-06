package nifi.arcgis.service.arcgis.services;

import com.esri.arcgisruntime.data.Field.Type.*;

import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WGS84;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WEBMERCATOR;

import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.bouncycastle.jcajce.provider.asymmetric.rsa.RSAUtil;

import com.esri.arcgisruntime.concurrent.ListenableFuture;
import com.esri.arcgisruntime.data.Feature;
import com.esri.arcgisruntime.data.FeatureEditResult;
import com.esri.arcgisruntime.data.Field;
import com.esri.arcgisruntime.data.ServiceFeatureTable;
import com.esri.arcgisruntime.geometry.Geometry;
import com.esri.arcgisruntime.geometry.GeometryType;
import com.esri.arcgisruntime.geometry.Point;
import com.esri.arcgisruntime.geometry.SpatialReference;
import com.esri.arcgisruntime.geometry.SpatialReferences;
import com.esri.arcgisruntime.loadable.LoadStatus;

import nifi.arcgis.service.arcgis.services.json.ArcGISServicesData;
import nifi.arcgis.service.arcgis.services.json.Layer;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE;

public class ArcGISDataManager {

	final static private String URL_REST_SERVICES = "/arcgis/rest/services/{0}?f=pjson";

	final static private String URL_REST_LAYERS = "/arcgis/rest/services/{0}/FeatureServer?f=pjson";

	final static private String URL_SERVICE_FEATURE_TABLE = "/arcgis/rest/services/{0}/FeatureServer/{1}";

	/**
	 * layer rank search and retrieve from the server with the name
	 */
	private int layerRank;

	/**
	 * Complete ArcGIS URL for accessing the featureServer
	 */
	private String featureTableCompleteUrl = null;

	/**
	 * Apache NIFI logger
	 */
	private final ComponentLog logger;

	/**
	 * Map of fields in the featureTable, loaded during the table initialization
	 */
	private final Map<String, ArcGISTableField> associateFields = new HashMap<String, ArcGISTableField>();

	/**
	 * Locker
	 */
	final static Locker locker = new Locker();

	/**
	 * Main constructor
	 * 
	 * @param nifiLogger
	 *            : Main logger plugged into the NIFI framework
	 */
	public ArcGISDataManager(ComponentLog nifiLogger) {
		this.logger = nifiLogger;
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

	/**
	 * Connector for managing the featureTable
	 */
	private ServiceFeatureTable featureTable = null;
	
	/*
	 * Check ArcGIS server connection with the current parameters.
	 * 
	 * @param arcgisURL URL of the ArcGIS server
	 * 
	 * @param folderServer Folder name starting from the root directory where
	 * the featureServers are located
	 * 
	 * @param featureServer Name of the FeatureServer
	 * 
	 * @param layerName Name of the layer inside the ArcGIS database
	 * 
	 * @return The validation report for these parameters
	 */
	public ValidationResult checkConnection(final String arcgisURL, final String folderServer,
			final String featureServer, final String layerName) {

		checkConnectionFinished = false;

		final ValidationResult.Builder builder = new ValidationResult.Builder();

		try {

			currentRestResource = arcgisURL;
			currentSubject = "arcGIS URL";
			new URL(currentRestResource);

			ArcGISServicesData dataArcGIS = new ArcGISServicesData(logger);

			if ((folderServer != null) && folderServer.isEmpty()) {
				currentRestResource = arcgisURL + MessageFormat.format(URL_REST_SERVICES, "");
				currentSubject = "folder server";

				Set<String> registeredFolders = dataArcGIS.retrieveFolderServer(currentRestResource);
				if (!registeredFolders.contains(folderServer)) {
					throw new Exception("The folder " + folderServer + " does not exist on the server " + arcgisURL);
				}

			}

			currentRestResource = arcgisURL + MessageFormat.format(URL_REST_SERVICES,
					((folderServer == null) || folderServer.isEmpty()) ? "" : folderServer);
			currentSubject = "featureLayer name";
			Set<String> registeredFeatureLayers = dataArcGIS.retrieveFeatureServer(currentRestResource);
			if (!registeredFeatureLayers.contains(featureServer)) {
				throw new Exception("The feature " + featureServer + " does not exist on the server " + arcgisURL);
			}

			currentRestResource = arcgisURL + MessageFormat.format(URL_REST_LAYERS, featureServer);
			currentSubject = "layer name";
			Set<Layer> registeredLayers = dataArcGIS.retrieveLayers(currentRestResource);
			Optional<Layer> oLayer = registeredLayers.stream().filter(layer -> layer.name.equals(layerName))
					.findFirst();
			if (!oLayer.isPresent()) {
				throw new Exception("The layer " + layerName + " does not exist on the featureServer " + featureServer
						+ " within the ArcGIS server " + arcgisURL);
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Found the layer ID " + oLayer.get().id + " for the layer name " + layerName);
			}
			layerRank = oLayer.get().id;

			currentRestResource = arcgisURL
					+ MessageFormat.format(URL_SERVICE_FEATURE_TABLE, featureServer, oLayer.get().id);
			featureTable = new ServiceFeatureTable(currentRestResource);
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

					if (!featureTable.isEditable()) {
						builder.input(featureTable.getTableName()).subject("layer name")
								.explanation(featureTable.getTableName() + " is read-only !").valid(false);
					} else {
						associateFields.clear();
						featureTable.getFields().forEach(field -> associateFields.put(field.getName(),
								new ArcGISTableField(field.getName(), field.getFieldType())));
						featureTableCompleteUrl = currentRestResource;
						builder.valid(true);
					}
				}
				if (ls.equals(LoadStatus.FAILED_TO_LOAD) || ls.equals(LoadStatus.NOT_LOADED)) {
					logger.debug("URL Load failed ", featureTable.getLoadError());
					logger.debug("URL Load failed cause ", featureTable.getLoadError().getCause());
					String errorMessage = (featureTable.getLoadError().getCause() == null)
							? featureTable.getLoadError().getMessage().toString()
							: featureTable.getLoadError().getCause().toString();
					builder.subject("url ArcGIS & layer name").input(arcgisURL).explanation(errorMessage).valid(false);
				}
				/*
				 * This status release the final infinite-loop before returning
				 * back to the NIFI framework
				 */
				checkConnectionFinished = true;
			};

			featureTable.addDoneLoadingListener(listener);

		} catch (final Exception e) {
			logger.error("Rest resource unreachable " + currentRestResource);
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

	/**
	 * @return return the layer ID retrieve in the ArcGIS server
	 */
	public int getLayerRank() {
		return layerRank;
	}

	/**
	 * @return the complete URL for accessing the current featureTable
	 */
	public String getFeatureTableCompleteUrl() {
		return featureTableCompleteUrl;
	}

	/**
	 * @return return the list of Fields loaded on the featureTable
	 */
	public Map<String, ArcGISTableField> getAssociateFields() {
		return associateFields;
	}

	/**
	 * Add a collection of features into the table town into the geo_db database
	 * 
	 * @param layerRank
	 *            the rank of the layer in the folder
	 * @param records
	 *            The list of records to add
	 * @param settings
	 *            the data settings associated, such as the current
	 *            SpatialReference
	 */
	public void processRecords(final List<Map<String, String>> records, final Map<String, Object> settings)
			throws Exception {

		if (featureTable == null) {
			if (logger.isDebugEnabled()) {
				logger.debug("working with the layer " + featureTableCompleteUrl);
			}
			featureTable = new ServiceFeatureTable(featureTableCompleteUrl);		
			featureTable.loadAsync();
		} else {
			if (featureTable.getLoadStatus() == LoadStatus.NOT_LOADED) {
				featureTable.loadAsync();
			} else{
				updateData(records, settings);
				return;
			}
		}

		featureTable.addDoneLoadingListener(() -> {
			LoadStatus ls = featureTable.getLoadStatus();

			if (ls.equals(LoadStatus.LOADED)) {
				try {
					updateData(records, settings);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				featureTable.getLoadError().printStackTrace();
			}
		});
	}
		
		/**
		 * Add a collection of features into the table town into the geo_db database
		 * 
		 * @param records
		 *            The list of records to add
		 * @param settings
		 *            the data settings associated, such as the current
		 *            SpatialReference
		 */
		public void updateData(final List<Map<String, String>> records, final Map<String, Object> settings)
				throws Exception {
		
		
		if (!featureTable.getGeometryType().equals(GeometryType.POINT)) {
			throw new RuntimeException("What's the fuck... Other geometries than point are not implemented yet !");
		}

		List<Feature> features = new ArrayList<Feature>();
		for (Map<String, String> record : records) {

			Geometry geometry = null;
			if (featureTable.getGeometryType().equals(GeometryType.POINT)) {
				geometry = createPoint(record, settings);
			}

			final Map<String, Object> attributTable = new HashMap<String, Object>();
			for (String field : record.keySet()) {
				String value = record.get(field);
				Object dataColumn;
				if (associateFields.containsKey(field)) {
					dataColumn = getDataColumn(field, value);
					attributTable.put(field, dataColumn);
				}
			}
			features.add(featureTable.createFeature(attributTable, geometry));
		}

		logger.debug("Adding " + features.size() + " features...");
		if (featureTable.canAdd()) {

			ListenableFuture<Void> res = featureTable.addFeaturesAsync(features);
			res.addDoneListener(() -> {
				try {
					res.get();
					if (res.isDone()) {
						featureTable.applyEditsAsync().addDoneListener(() -> applyEdits(featureTable));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} else {
			new Exception("Cannot add feature into " + featureTable.getTableName()).printStackTrace();
		}
	}

	/**
	 * Instantiate a data candidate.
	 * 
	 * @param field
	 *            the field name
	 * @param value
	 *            the value loaded from the file, which has to be add
	 * @return the value object
	 * @throws Exception
	 *             : exception occurs during the data parsing.
	 *             NumberFormatException might be thrown.
	 */
	private Object getDataColumn(String field, String value) throws Exception {
		Object o = null;
		if (associateFields.containsKey(field)) {
			switch (associateFields.get(field).type) {
			case DOUBLE:
				o = Double.parseDouble(value);
				break;
			case INTEGER:
				o = Integer.parseInt(value);
				break;
			case TEXT:
				o = value;
				break;
			default:
				throw new Exception("Not implemented yet for " + associateFields.get(field).type);
			}
		}
		return o;
	}

	/**
	 * Sends any edits on the ServiceFeatureTable to the server.
	 *
	 * @param featureTable
	 *            service feature table
	 */
	private void applyEdits(ServiceFeatureTable featureTable) {

		// apply the changes to the server
		ListenableFuture<List<FeatureEditResult>> editResult = featureTable.applyEditsAsync();
		editResult.addDoneListener(() -> {
			try {
				List<FeatureEditResult> edits = editResult.get();
				// check if the server edit was successful
				if (edits != null && edits.size() > 0 && edits.get(0).hasCompletedWithErrors()) {
					throw edits.get(0).getError();
				}
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			} finally {
				locker.unlock();
			}
		});
	}

	/**
	 * <p>
	 * Create an ArcGIS point. <br/>
	 * The constructor used will hang on the presence of fields such as "x",
	 * "y", "z. <br/>
	 * If not field such as "latitude" and "longitude" will be used. <br/>
	 * If any information is available to allow the creation of a point, an
	 * exception will be thrown.
	 * </p>
	 * <p>
	 * <i>The parameter-value "m" is not handled in this current release</i>
	 * </p>
	 *
	 * @return
	 */
	public Geometry createPoint(Map<String, String> records, Map<String, Object> settings) throws Exception {

		if (!((records.containsKey("x") && records.containsKey("y"))
				|| (records.containsKey("lattitude") && records.containsKey("longitude")))) {
			throw new Exception("Cannot create a point based on the received data");
		}

		SpatialReference spatialReference = null;
		if (settings.containsKey(SPATIAL_REFERENCE)) {
			String refSpatialReference = (String) settings.get(SPATIAL_REFERENCE);
			if (SPATIAL_REFERENCE_WGS84.equals(refSpatialReference)) {
				spatialReference = SpatialReferences.getWgs84();
			} else {
				if (SPATIAL_REFERENCE_WEBMERCATOR.equals(refSpatialReference)) {
					spatialReference = SpatialReferences.getWebMercator();
				} else {
					throw new Exception(refSpatialReference + " is an invalid SpatialReference !");
				}
			}
		}

		if (records.containsKey("x") && records.containsKey("y")) {
			double x = Double.parseDouble(records.get("x"));
			double y = Double.parseDouble(records.get("y"));
			if (records.containsKey("z")) {
				double z = Double.parseDouble(records.get("z"));
				return (spatialReference == null) ? new Point(x, y, z) : new Point(x, y, z, spatialReference);
			}
			return (spatialReference == null) ? new Point(x, y) : new Point(x, y, spatialReference);
		}
		if (records.containsKey("longitude") && records.containsKey("lattitude")) {
			double lattitude = Double.parseDouble(records.get("lattitude"));
			double longitude = Double.parseDouble(records.get("longitude"));
			return (spatialReference == null) ? new Point(lattitude, longitude)
					: new Point(lattitude, longitude, spatialReference);
		}
		throw new Exception("Should not pass here!");
	}

}
