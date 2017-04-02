package nifi.arcgis.service.arcgis.services;

import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WEBMERCATOR;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WGS84;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.TYPE_OF_QUERY;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.TYPE_OF_QUERY_GEO;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_UPDATE;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_UPDATE_OR_INSERT;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.UPDATE_FIELD_LIST;

import static com.jayway.awaitility.Awaitility.await;

import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;

import com.esri.arcgisruntime.concurrent.ListenableFuture;
import com.esri.arcgisruntime.data.ArcGISFeature;
import com.esri.arcgisruntime.data.Feature;
import com.esri.arcgisruntime.data.FeatureEditResult;
import com.esri.arcgisruntime.data.FeatureQueryResult;
import com.esri.arcgisruntime.data.FeatureTable;
import com.esri.arcgisruntime.data.Field;
import com.esri.arcgisruntime.data.QueryParameters;
import com.esri.arcgisruntime.data.QueryParameters.SpatialRelationship;
import com.esri.arcgisruntime.data.ServiceFeatureTable;
import com.esri.arcgisruntime.geometry.Geometry;
import com.esri.arcgisruntime.geometry.GeometryEngine;
import com.esri.arcgisruntime.geometry.GeometryType;
import com.esri.arcgisruntime.geometry.Point;
import com.esri.arcgisruntime.geometry.Polygon;
import com.esri.arcgisruntime.geometry.SpatialReference;
import com.esri.arcgisruntime.geometry.SpatialReferences;
import com.esri.arcgisruntime.layers.FeatureLayer;
import com.esri.arcgisruntime.layers.FeatureLayer.SelectionMode;
import com.esri.arcgisruntime.loadable.LoadStatus;
import com.jayway.awaitility.Duration;
import com.jayway.awaitility.core.ConditionTimeoutException;

import nifi.arcgis.service.arcgis.services.json.ArcGISServicesData;
import nifi.arcgis.service.arcgis.services.json.Layer;

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
	 * Main constructor
	 * 
	 * @param nifiLogger
	 *            : Main logger plugged into the NIFI framework
	 */
	public ArcGISDataManager(ComponentLog nifiLogger) {
		this.logger = nifiLogger;
	}

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

	/**
	 * Connector for managing a layer in the featureTable.
	 */
	private FeatureLayer featureLayer = null;

	/*
	 * Default radius for the search mechanism. This value is supposed to be
	 * overridden with the passed setting
	 */
	private final int DEFAULT_RADIUS = 10;

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

		// TODO Can we trust on theses 2 conditions ?
		if ((featureLayer != null) && (featureTable != null)) {
			return new ValidationResult.Builder().valid(true).build();
		}

		// This variable is used to verify that the data operation is terminated
		final AtomicBoolean dataOperationTerminated = new AtomicBoolean(false);

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
				dataOperationTerminated.set(true);
			};

			featureTable.addDoneLoadingListener(listener);

		} catch (final Exception e) {
			logger.error("Rest resource unreachable " + currentRestResource);
			logger.error(ExceptionUtils.getStackTrace(e));
			return builder.input(currentRestResource).explanation(e.getMessage()).valid(false).subject(currentSubject)
					.build();

		}

		await().untilTrue(dataOperationTerminated);
		featureLayer = new FeatureLayer(featureTable);

		// TODO INTERUPT EXCEPTION HAS TO BE HANDLE
		// return
		// builder.input(currentRestResource).explanation(e.getMessage()).valid(false).subject(currentSubject).build();

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
			} else {
				insertData(records, settings);
				return;
			}
		}

		featureTable.addDoneLoadingListener(() -> {
			LoadStatus ls = featureTable.getLoadStatus();

			if (ls.equals(LoadStatus.LOADED)) {
				try {
					insertData(records, settings);
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

		if (!TYPE_OF_QUERY_GEO.equals(settings.get(TYPE_OF_QUERY))) {
			throw new RuntimeException("WTF : Type of query unknown " + settings.get(TYPE_OF_QUERY));
		}
		for (Map<String, String> record : records) {

			ArcGISFeature feature;
			try {
				feature = geoQuery(record, settings);
			} catch (final ConditionTimeoutException cte) {
				feature = null;
			}
			if (feature == null) {

				if (OPERATION_UPDATE.equals(settings.get(OPERATION))) {
					throw new Exception("Cannot update this data. Record does not exist on the target featureTable");
				}

				// We add one single record
				// This is not the most effective way to handle this use case
				if (OPERATION_UPDATE_OR_INSERT.equals(settings.get(OPERATION))) {
					List<Map<String, String>> singleRecordToAdd = new ArrayList<Map<String, String>>();
					singleRecordToAdd.add(record);
					insertData(singleRecordToAdd, settings);
					continue;
				}
			}

			Map<String, Object> attributes = feature.getAttributes();

			RecordAttributeDataOperation recordOperation = new RecordAttributeDataOperation(record);
			List<String> fieldsToUpdate = (List<String>) settings.get(UPDATE_FIELD_LIST);
			if (fieldsToUpdate == null) {
				throw new Exception ("Settings object does not provide an update fields list (Key:UPDATE_FIELD_LIST)" );
			}
			fieldsToUpdate.forEach(fieldToUpdateWithOperator -> {
				String fieldToUpdate = fieldNameUndecorated(fieldToUpdateWithOperator);
				if (logger.isDebugEnabled()) {
					logger.debug(fieldToUpdateWithOperator + " in db:" + attributes.get(fieldToUpdate) + " param:"
							+ record.get(fieldToUpdate));
				}
				Object data = null;
				if (record.get(fieldToUpdate) != null) {
					try {
						data = getDataColumn(fieldToUpdate, record.get(fieldToUpdate));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					final BiFunction<String, ? super Object, ? extends Object> dataOperation = getBiFunction(
							fieldToUpdateWithOperator, recordOperation, data);
					if (logger.isDebugEnabled()) {
						logger.debug("data " + data.getClass() + ", associated operation " + dataOperation.getClass());
					}
					logger.debug(attributes.get(fieldToUpdate).toString());
					if (attributes.get(fieldToUpdate) != null) {
						attributes.computeIfPresent(fieldToUpdate, dataOperation);
					} else {
						attributes.put(fieldToUpdate, data);
					}
				}
			});

			logger.debug("Applying edition");
			AtomicBoolean done = new AtomicBoolean(false);
			featureTable.updateFeatureAsync(feature).addDoneListener(() -> {
				done.set(true);
			});
			await().untilTrue(done);
		}

		applyEdits(featureTable);
	}

	/**
	 * Take off the operator if any, present in the first position of the
	 * string. <code>+hit</code> will become <code>hit</ccreatode>
	 * 
	 * @param fieldName
	 *            name of the field
	 * @return the field name undecorated
	 */
	private String fieldNameUndecorated(final String fieldName) {
		return ("+-".indexOf(fieldName.charAt(0)) == -1) ? fieldName : fieldName.substring(1);
	}

	/**
	 * For Numeric field only, this function returns the operation to be
	 * executed with the previous and next data value.
	 * 
	 * @param fieldToUpdate
	 *            field name to update in the featureTable
	 * @param dataOperation
	 *            the single unite data operation manager
	 * @param dataValueToUpdate
	 *            data for update
	 */
	static BiFunction<String, ? super Object, ? extends Object> getBiFunction(final String fieldToUpdate,
			RecordAttributeDataOperation dataOperation, final Object dataValueToUpdate) {

		if (dataValueToUpdate instanceof String) {
			return dataOperation.setString;
		}

		char operator = fieldToUpdate.charAt(0);
		// The is no operator. This is just an affectation
		if ("+-".indexOf(operator) == -1) {
			if (dataValueToUpdate instanceof Integer) {
				return dataOperation.setInteger;
			}
			if (dataValueToUpdate instanceof Double) {
				return dataOperation.setDouble;
			}
			throw new RuntimeException(dataValueToUpdate.getClass().getName() + " is not implemented yet!");
		}

		if (operator == '+') {
			if (dataValueToUpdate instanceof Integer) {
				return dataOperation.addInteger;
			}
			if (dataValueToUpdate instanceof Double) {
				return dataOperation.addDouble;
			}
			throw new RuntimeException(dataValueToUpdate.getClass().getName() + " is not implemented yet!");
		}

		if (operator == '-') {
			if (dataValueToUpdate instanceof Integer) {
				return dataOperation.subtractInteger;
			}
			if (dataValueToUpdate instanceof Double) {
				return dataOperation.subtractDouble;
			}
			throw new RuntimeException(dataValueToUpdate.getClass().getName() + " is not implemented yet!");
		}

		throw new RuntimeException("WTF : Should not pass here");
	}

	/**
	 * Reinitialize the featureLayer. <br/>
	 * This re-initialization is executed to prevent the call <br/>
	 * <code>FeatureLayer featureLayer = new FeatureLayer(featureTable);</code>
	 * <br/>
	 * from the error message <b>"data_source is already owned."</b> <br/>
	 * <i>This method is public in order to be invoked from Unit test</i>
	 */
	public void reinitializeFeatureTable() {
		final AtomicBoolean reloadDone = new AtomicBoolean(false);
		if ((featureTable != null) && (featureTable.getLoadStatus() == LoadStatus.LOADED)) {
			featureTable.clearCache(false);
		}
		String uri = featureTable.getUri();
		featureTable = new ServiceFeatureTable(uri);
		featureTable.loadAsync();
		featureTable.addDoneLoadingListener(() -> reloadDone.set(true));
		await().untilTrue(reloadDone);
	}

	/**
	 * Select a record in the featureTable in a circle around a latitude and a
	 * longitude.
	 * 
	 * @param record
	 *            actual record for processing
	 * @param settings
	 *            current settings of data management
	 * @return the selected feature
	 * @throws Exception
	 */
	ArcGISFeature geoQuery(Map<String, String> record, final Map<String, Object> settings) throws Exception {

		SpatialReference spatialReference = getSpatialReference(settings);

		if (!(featureTable.getGeometryType().equals(GeometryType.POINT))) {
			throw new RuntimeException("WTF SHOULD NOT PASS HERE !");
		}
		final Point geometry = createPoint(record, settings);
		
		


	int radius = (settings.containsKey(ArcGISLayerServiceAPI.RADIUS))
				? (Integer) settings.get(ArcGISLayerServiceAPI.RADIUS) : DEFAULT_RADIUS;
				
		final Polygon searchAround = GeometryEngine.buffer(geometry, radius);
		
		QueryParameters queryParams = new QueryParameters();
		queryParams.setGeometry(searchAround);
		queryParams.setOutSpatialReference(spatialReference);
		queryParams.setSpatialRelationship(SpatialRelationship.INTERSECTS);
		
		final ListenableFuture<FeatureQueryResult> future = featureLayer.selectFeaturesAsync(queryParams,
				SelectionMode.NEW);

		// Selected feature returned by this function
		final AtomicReference<ArcGISFeature> selectedFeature = new AtomicReference<ArcGISFeature>();

		// The purpose of queryDone is to wait for query completion before
		// returning the result
		AtomicBoolean queryDone = new AtomicBoolean(false);
		future.addDoneListener(() -> {
			
			final ComponentLog logger = ArcGISDataManager.this.logger;
			FeatureQueryResult result;
			try {
				result = future.get();
			} catch (Exception e) {
				logger.error(ExceptionUtils.getStackTrace(e));
				throw new RuntimeException(e);
			}
			final AtomicReference<Double> closestDistance = new AtomicReference<Double>(new Double(Double.MAX_VALUE));
			result.forEach( feature -> {
				ArcGISFeature arcgisfeature = (ArcGISFeature) feature;
				logger.debug("before distance calculation for the radius " + radius);
				logger.debug(arcgisfeature.getGeometry().toJson());
				if (arcgisfeature.getGeometry().getGeometryType() != GeometryType.POINT) {
					throw new RuntimeException("WTF: Should not pass here. Unattempted type of geometry " + arcgisfeature.getGeometry());
				} 
				Point point = (Point) arcgisfeature.getGeometry();
				double distance = ArcGISDataManager.distance (geometry.getX(), point.getX(), geometry.getY(), point.getY(), 0d, 0d); 
				logger.debug("distance " + distance);
				if (distance < closestDistance.get()) {
					selectedFeature.set((ArcGISFeature) arcgisfeature);
					closestDistance.set(distance);
					if (logger.isDebugEnabled()) {
						Map<String, Object> attributes = selectedFeature.get().getAttributes();
						logger.debug("Feature select " + attributes.get("name") + " @ the distance "
								+ closestDistance.get());
					}
				}
			});
			queryDone.set(true);
		});
		await().untilTrue(queryDone);

		if (selectedFeature.get() != null) {
			loadFeature (selectedFeature.get());
		}
		
		return selectedFeature.get();
	}

	private void loadFeature(ArcGISFeature feature) {
		AtomicBoolean queryDone = new AtomicBoolean(false);
		feature.loadAsync();
		feature.addDoneLoadingListener(() -> queryDone.set(true));
		await().untilTrue(queryDone);
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
	public void insertData(final List<Map<String, String>> records, final Map<String, Object> settings)
			throws Exception {

		final AtomicBoolean dataOperationTerminated = new AtomicBoolean(false);
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
						final AtomicBoolean dataEditionTerminated = new AtomicBoolean(false);
						featureTable.applyEditsAsync().addDoneListener(() -> {
							applyEdits(featureTable);
							dataEditionTerminated.set(true);
						});
						await().untilTrue(dataEditionTerminated);
					}
				} catch (Exception e) {
					ArcGISDataManager.this.logger
							.error("Error while adding FeaturesAsync :\\n" + ExceptionUtils.getStackTrace(e));

				} finally {
					dataOperationTerminated.set(true);
				}

			});
			await().untilTrue(dataOperationTerminated);
			logger.debug("dataOperationTerminated");

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
	Object getDataColumn(String field, String value) throws Exception {
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

		final AtomicBoolean editionApplied = new AtomicBoolean(false);

		// apply the changes to the server
		ListenableFuture<List<FeatureEditResult>> editResult = featureTable.applyEditsAsync();
		editResult.addDoneListener(() -> {
			try {
				List<FeatureEditResult> edits = editResult.get();
				ArcGISDataManager.this.logger.debug("Edition applied (edits.size=" + edits.size() + ")");
				// check if the server edit was successful
				if (edits != null && edits.size() > 0 && edits.get(0).hasCompletedWithErrors()) {
					throw edits.get(0).getError();
				}

			} catch (InterruptedException | ExecutionException e) {
				ArcGISDataManager.this.logger
						.error("Error while adding FeaturesAsync :\\n" + ExceptionUtils.getStackTrace(e));
			}
			editionApplied.set(true);
		});

		// TODO InterruptedException has to be handled
		await().untilTrue(editionApplied);

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
	 * @param record
	 *            actual record
	 * @setting general settings associated to this record
	 * @return a point
	 * @throws Exception
	 */
	public Point createPoint(Map<String, String> record, Map<String, Object> settings) throws Exception {

		if (!((record.containsKey("x") && record.containsKey("y"))
				|| (record.containsKey("latitude") && record.containsKey("longitude")))) {
			throw new Exception("Cannot create a point based on the received data");
		}

		SpatialReference spatialReference = getSpatialReference(settings);

		if (record.containsKey("x") && record.containsKey("y")) {
			double x = Double.parseDouble(getNotNull(record,"x"));
			double y = Double.parseDouble(getNotNull(record,"y"));
			if (record.containsKey("z")) {
				double z = Double.parseDouble(getNotNull(record,"z"));
				return (spatialReference == null) ? new Point(x, y, z) : new Point(x, y, z, spatialReference);
			}
			return (spatialReference == null) ? new Point(x, y) : new Point(x, y, spatialReference);
		}
		if (record.containsKey("longitude") && record.containsKey("latitude")) {
			double lattitude = Double.parseDouble(getNotNull(record,"latitude"));
			double longitude = Double.parseDouble(getNotNull(record,"longitude"));
			return (spatialReference == null) ? new Point(lattitude, longitude)
					: new Point(lattitude, longitude, spatialReference);
		}
		throw new Exception("Should not pass here!");
	}

	/**
	 * Test and return the value mapped to the key.
	 * <br/>If the value is null, an exception is thrown
	 * @param record the record in a Map format
	 * @param key the key searched
	 * @return the non null value
	 * @throws Exception if the value map to the key is null
	 */
	private String getNotNull(Map<String, String> record, String key) throws Exception {
		String value = record.get(key);
		if (record.get(key) == null) {
			Exception e = new Exception ("key value is null " + key);
			record.forEach((k,v)->{ logger.error(k); logger.error(v); });
			throw e;
		} else {
			return value;
		}			
	}
	/**
	 * @param settings
	 *            the actual data settings setup in the processor
	 * @return the SpatialReference of the data
	 */
	private SpatialReference getSpatialReference(final Map<String, Object> settings) throws Exception {

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
		return spatialReference;
	}

	/**
	 * <font color="red"> INFO : Using this method instead of the
	 * <code>GeometryEngine.distanceBetween</code> available in the ArcGIS java
	 * client. <br/>
	 * NIFI is freezing during execution, for an unknown reason. </br>
	 * This behavior cannot be reproduced with a JUnit test. <br/>
	 * <br/>
	 * </font>
	 * 
	 * Calculate distance between two points in latitude and longitude taking
	 * into account height difference. If you are not interested in height
	 * difference pass 0.0.
	 * 
	 * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
	 * el2 End altitude in meters
	 * 
	 * @returns Distance in Meters
	 */
	public static double distance(double lat1, double lat2, double lon1, double lon2, double el1, double el2) {

		final int R = 6371; // Radius of the earth

		Double latDistance = Math.toRadians(lat2 - lat1);
		Double lonDistance = Math.toRadians(lon2 - lon1);
		Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = R * c * 1000; // convert to meters

		double height = el1 - el2;

		distance = Math.pow(distance, 2) + Math.pow(height, 2);

		return Math.sqrt(distance);
	}

}
