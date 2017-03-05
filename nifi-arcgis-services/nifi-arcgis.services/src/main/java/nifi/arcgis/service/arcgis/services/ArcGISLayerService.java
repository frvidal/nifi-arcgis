/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifi.arcgis.service.arcgis.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "ArcGIS", "put" })
@CapabilityDescription("ControllerService in charge of accessing a featureTable on an ArcGIS server.")
public class ArcGISLayerService extends AbstractControllerService implements ArcGISLayerServiceAPI {

	public static final PropertyDescriptor ARCGIS_URL = new PropertyDescriptor.Builder()
			.name("URL of the ArcGIS server").description("ArcGIS server root URL (ex: http://hostname:6080).")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.addValidator(StandardValidators.URL_VALIDATOR).build();

	public static final PropertyDescriptor FOLDER_SERVER = new PropertyDescriptor.Builder()
			.name("Folder of the FeatureServers")
			.description("Storage folder starting from root, where featureServers are located on the ArcGIS server")
			.required(false).addValidator((String subject, String input, ValidationContext context) -> {
				return new ValidationResult.Builder().valid(true).build();
			}).build();

	public static final PropertyDescriptor FEATURE_SERVER = new PropertyDescriptor.Builder()
			.name("Name of the FeatureServer").description("Feature server avaible on the ArcGIS server").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor LAYER_NAME = new PropertyDescriptor.Builder().name("Name of the layer")
			.description("Layer name avaible on the FeatureServer").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	/**
	 * This boolean is used to test the entrance in the OnPropertyChange method
	 * for test purpose
	 */
	private boolean opmCalled = false;

	private static final List<PropertyDescriptor> properties;

	/**
	 * working layer rank searched, and retrieved from the ArcGIS Server by its name
	 */
	private int layerRank;
	
	/**
	 * List of tableFields available on this current working layer
	 */
	private Map<String, ArcGISTableField> tableFields;

	static {
		final List<PropertyDescriptor> props = new ArrayList<>();
		props.add(ARCGIS_URL);
		props.add(FOLDER_SERVER);
		props.add(FEATURE_SERVER);
		props.add(LAYER_NAME);
		properties = Collections.unmodifiableList(props);
	}

	@Override
	public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
		super.onPropertyModified(descriptor, oldValue, newValue);

		if (getLogger().isDebugEnabled()) {
			getLogger().debug("onPropertyModified for " + descriptor.getName() + " : oldValue=" + oldValue
					+ ", newValue=" + newValue);
		}
		/*
		 * 
		 * - A directory specified by calling
		 * ArcGISRuntimeEnvironment.setInstallDirectory() - The current
		 * directory /usr/local/nifi-1.1.0 - A location specified by the
		 * environment variable ARCGISRUNTIMESDKJAVA_100_0_0 - Within the
		 * ".arcgis" directory in the user's home path /root/.arcgis
		 */
		/*
		 * if (descriptor.equals (ARCGIS_URL)) { getLogger().info(
		 * "Connecting to the URL " + newValue); }
		 */

		opmCalled = true;
	}

	List<ValidationResult> results = new ArrayList<ValidationResult>();

	@Override
	/**
	 * Check the validity of the ArcGIS server GIS URL and
	 * 
	 * @param url
	 *            Rest URL of an ArcGIS server
	 * @return a collection of ValidationResult
	 */
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

		PropertyValue url = validationContext.getProperty(ARCGIS_URL);
		PropertyValue folderServer = validationContext.getProperty(FOLDER_SERVER);
		if (folderServer == null) {
			getLogger().debug("folderServer is null!");
		}

		PropertyValue featureServer = validationContext.getProperty(FEATURE_SERVER);
		PropertyValue layerName = validationContext.getProperty(LAYER_NAME);
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("validating the URL " + url.toString()
					+ ((folderServer == null) ? "" : (" in the folder " + folderServer.getValue()))
					+ " for the featureServer " + featureServer.getValue() + " and the layer " + layerName.getValue());
		}
		List<ValidationResult> results = new ArrayList<ValidationResult>();
		ArcGISDataManager gisDataManager = new ArcGISDataManager(getLogger());
		ValidationResult result = gisDataManager.checkConnection(url.getValue(), folderServer.getValue(),
				featureServer.getValue(), layerName.getValue());
		if (result.isValid()) {
			layerRank = gisDataManager.getLayerRank();
			tableFields = gisDataManager.getAssociateFields();
		}
		// results.clear(); ?
		results.add(result);
		return results;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	/**
	 * @param context
	 *            the configuration context
	 * @throws InitializationException
	 *             if unable to create a database connection
	 */
	@OnEnabled
	public void onEnabled(final ConfigurationContext context) throws InitializationException {
	}

	@OnDisabled
	public void shutdown() {

	}


	@Override
	protected void init(ControllerServiceInitializationContext config) throws InitializationException {
		// TODO Auto-generated method stub
		super.init(config);
	}

	/**
	 * For testing purpose. In charge of detecting the invocation of
	 * <code>onPropertyModifier</code>.
	 * 
	 * @return
	 */
	public boolean isOpmCalled() {
		return opmCalled;
	}

	@Override
	public boolean isHeaderValid(List<String> header) throws ProcessException {
		
		if (getLogger().isDebugEnabled()) {
			getLogger().debug ("Comparing header fields in the CSV and dataField in the ArcGIS FeatureTable");
			StringBuilder sbArcGIS = new StringBuilder();
			tableFields.forEach( (key, field) -> sbArcGIS.append(field.name).append(","));
			getLogger().debug ("ArcGIS list : " + sbArcGIS.toString());
			StringBuilder sbHeader = new StringBuilder();
			header.forEach(field -> sbHeader.append(field).append(","));
			getLogger().debug ("Header list : " + sbHeader.toString());
		}

		final AtomicReference<Boolean> found = new AtomicReference<Boolean>();
		found.set(false);
		for (String headerField : header) {
			found.set(false);
			tableFields.forEach( (key,field) -> { if (field.name.equals(headerField)) found.set(true); } );
			if (!found.get()) {
				getLogger().info("field " + headerField + " does not exist on this layer");
				break;
			}
		}
		return found.get();
	}

	@Override
	public void execute( List<Map<String, String>> records, final Map<String,Object> settings) throws ProcessException {
		getLogger().debug ("executing the update");
		ArcGISDataManager gisDataManager = new ArcGISDataManager(getLogger());
		
		try {
			gisDataManager.updateData(records, settings);
		} catch (Exception e) {
			getLogger().error(e.getMessage());
			throw new ProcessException(e.getMessage());
		}
	}

}
