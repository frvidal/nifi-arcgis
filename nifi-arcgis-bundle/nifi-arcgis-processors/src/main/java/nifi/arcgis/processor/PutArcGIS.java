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
package nifi.arcgis.processor;

import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WEBMERCATOR;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WGS84;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.esri.arcgisruntime.internal.jni.de;
import com.esri.arcgisruntime.internal.jni.ex;

import nifi.arcgis.processor.utility.CsvManager;
import nifi.arcgis.processor.utility.FileManager;
import nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI;

/**
 * @author frvidal
 *
 */
@Tags({ "put", "ArcGIS" })
@CapabilityDescription("Sends the contents of a FlowFile into a feature table on an ArcGIS server.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class PutArcGIS extends AbstractProcessor {

	private final static int QUOTITY_DEFAULT = 5000;
	
	private final static String JSON = "JSON";
	private final static String CSV = "CSV";
	private final static String FILE_ENCODING = "UTF-8";
	
	public static final PropertyDescriptor ARCGIS_SERVICE = new PropertyDescriptor.Builder().name("ArcGIS server")
			.description("This Controller Service to use in order to access the ArcGIS server").required(true)
			.identifiesControllerService(nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.class).build();

	public static final PropertyDescriptor TYPE_OF_FILE = new PropertyDescriptor.Builder().name("Type of file")
			.description("Type of file to import into ArcGIS\nCSV files require a header with the target column name")
			.required(true).allowableValues(CSV, JSON).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor SPATIAL_REFERENCE = new PropertyDescriptor.Builder().name("Spatial reference")
			.description("Type of spatial reference if necessary").allowableValues(SPATIAL_REFERENCE_WGS84,SPATIAL_REFERENCE_WEBMERCATOR).required(false).build();
	
	public static final PropertyDescriptor QUOTITY = new PropertyDescriptor.Builder().name("Quotity")
			.description("Quotity of records to proceed (1 by 1, n by n)").defaultValue(String.valueOf(QUOTITY_DEFAULT)).addValidator(StandardValidators.INTEGER_VALIDATOR).required(false).build();
	
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Success relationship").build();

	public static final Relationship FAILED = new Relationship.Builder().name("FAILED")
			.description("Failed relationship").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	/**
	 * Counter declared as a field inside the class in order to be used and modified inside lambda expression
	 */
	private int counter = 0;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(ARCGIS_SERVICE);
		descriptors.add(TYPE_OF_FILE);
		descriptors.add(SPATIAL_REFERENCE);
		descriptors.add(QUOTITY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILED);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

		final String typeOfFile = context.getProperty(TYPE_OF_FILE).getValue();
		if (JSON.equals(typeOfFile)) {
			getLogger().error(JSON + " file workflow is not implemented yet !");
			session.transfer(session.get(), FAILED);
			return;
		}

		final AtomicReference<Map<Integer, List<String>>> result = new AtomicReference<Map<Integer, List<String>>>();

		final FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		getLogger().debug("onTrigger");
		Map<String, String> data = flowFile.getAttributes();
		data.keySet().forEach(key -> getLogger().debug(key + " " + data.get(key)));
		if (CSV.equals(typeOfFile)) {

			session.read(flowFile, (InputStream inputStream) -> {
				Map<Integer, List<String>> lines = new HashMap<Integer, List<String>>();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(inputStream, Charset.forName(FILE_ENCODING)));
				StringBuilder sb;
				int lineNumber = 0;
				while ((sb = FileManager.readLine(reader)) != null) {
					getLogger().debug("parsing the CSV line " + sb.toString());
					List<String> line = CsvManager.parseLine(sb.toString(), ';');
					lines.put(lineNumber++, line);
				}
				result.set(lines);
			});

			getLogger().debug("Number of lines read " + String.valueOf(result.get().size()));
			
			List<String> columnNames = result.get().remove(0);
			ArcGISLayerServiceAPI service = context.getProperty(ARCGIS_SERVICE).asControllerService(ArcGISLayerServiceAPI.class);
			boolean headerValid = service.isHeaderValid(columnNames);
			// The first line in the CSV files does not contain a valid list of columns inside the featureTable
			if (!headerValid) {
				StringBuffer sb = new StringBuffer();
				columnNames.forEach(column -> sb.append(column).append(","));
				getLogger().error("File header invalid : " + sb.toString());
				session.transfer(flowFile, FAILED);
				return;
			}
			
			List<Map<String, String>> records = new ArrayList<Map<String, String>>();
			
			result.get().forEach((key, values) -> {
				counter = 0;
				Map<String, String> record = new HashMap<String, String>();
				// counter is used to be mapped with the list of column names parsed from the first line
				// counter is a field in order to be modified in the lambda expression
				values.forEach(value -> record.put(columnNames.get(counter++), value) );
				records.add (record);
				if (getLogger().isDebugEnabled()) {
					record.forEach((col, value) -> getLogger().debug(col + " : " + value));
				}
			});
			
			Map<String, Object> settings = new HashMap<String, Object>();
			final String spatialReference = context.getProperty(SPATIAL_REFERENCE).getValue();
			if (spatialReference != null && spatialReference.length() >0) {
				settings.put(SPATIAL_REFERENCE.getName(), spatialReference);
			}
			
			int quotity = Integer.valueOf(context.getProperty(QUOTITY).getValue());
			final int nb_total_records = records.size();
			getLogger().debug("Processing " + nb_total_records + " records by blocks of " + String.valueOf(quotity) + " elements");
			
			while (!records.isEmpty()) {
				List<Map<String, String>> processingRecords = new ArrayList<Map<String, String>>();
				for (int i = 0; i < quotity; i++) {
					processingRecords.add(records.remove(0));
					if (records.isEmpty())
						break;
				}
				try {
					getLogger().debug("Processing " + processingRecords.size() + " records...");
					service.execute(processingRecords, settings);
					getLogger().debug("..." + processingRecords.size() + " records processed");
				} catch (final ProcessException pe) {
					getLogger().error(ExceptionUtils.getStackTrace(pe));
					if (pe.getCause() != null) {
						getLogger().error(ExceptionUtils.getStackTrace(pe.getCause()));
					}
					session.transfer(flowFile, FAILED);
					return;
				}
			}
			getLogger().debug("At all " + nb_total_records + " records processed");
			
		} else {
			throw new RuntimeException ("What's The Fuck... Should not pass here !");
		}

		session.transfer(flowFile, SUCCESS);
	}
}
