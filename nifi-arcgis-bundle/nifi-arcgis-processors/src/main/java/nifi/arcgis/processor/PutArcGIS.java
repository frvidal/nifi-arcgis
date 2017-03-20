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
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.UPDATE_FIELD_LIST;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_INSERT;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_UPDATE;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_UPDATE_OR_INSERT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import nifi.arcgis.processor.utility.CsvManager;
import nifi.arcgis.processor.utility.FileManager;
import nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI;

/**
 * Processor for ArcGIS
 * ).
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
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
	private final static String ATTRIBUTE = "Attribute";
	
	private final static String DEFAULT_CHARACTER_SET = "UTF-8";

	public static final PropertyDescriptor ARCGIS_SERVICE = new PropertyDescriptor.Builder().name("ArcGIS server")
			.description("This Controller Service to use in order to access the ArcGIS server").required(true)
			.identifiesControllerService(nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.class).build();

	public static final PropertyDescriptor TYPE_OF_FILE = new PropertyDescriptor.Builder().name("Type of file")
			.description("Type of file to import into ArcGIS\nCSV files require a header with the target column name")
			.required(true).allowableValues(CSV, JSON, ATTRIBUTE).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor SPATIAL_REFERENCE = new PropertyDescriptor.Builder()
			.name("Spatial reference").description("Type of spatial reference if necessary")
			.allowableValues(SPATIAL_REFERENCE_WGS84, SPATIAL_REFERENCE_WEBMERCATOR).required(false).build();

	public static final PropertyDescriptor QUOTITY = new PropertyDescriptor.Builder().name("Quotity")
			.description("Quotity of records to proceed (1 by 1, n by n)").defaultValue(String.valueOf(QUOTITY_DEFAULT))
			.addValidator(StandardValidators.INTEGER_VALIDATOR).required(false).build();

	public static final PropertyDescriptor CHARACTER_SET_IN = new PropertyDescriptor.Builder().name("Character Set IN")
			.description("Character Set IN").defaultValue(DEFAULT_CHARACTER_SET)
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).required(true).build();

	public static final PropertyDescriptor TYPE_OF_DATA_OPERATION = new PropertyDescriptor.Builder()
			.name(OPERATION).description("Data operation to be done on the ArcGIS server")
			.allowableValues(OPERATION_INSERT, OPERATION_UPDATE, OPERATION_UPDATE_OR_INSERT).required(true).build();

	public static final PropertyDescriptor FIELD_LIST_INSERT = new PropertyDescriptor.Builder()
			.name("File containing the complete list of fields involved in data INSERTION")
			.description("The file have to contain the columns list to INSERT in the featureTable")
			.addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).required(true).build();

	public static final PropertyDescriptor FIELD_LIST_UPDATE = new PropertyDescriptor.Builder()
			.name("File containing the complete list of fields involved in data EDITION")
			.description("The file have to contain the columns list to EDIT in the featureTable.\\n"
					+ "FOR CSV file : If this field is empty, the processor will use the header of the CSV file.\\n"
					+ "FOR JSON file : If this field is empty, all fields present in the JSON file are candidate for edition")
			.addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).required(false).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Success relationship").build();

	public static final Relationship FAILED = new Relationship.Builder().name("FAILED")
			.description("Failed relationship").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;
	
	/** 
	 * Fields list to parse and send to the processor service the the data operation
	 */
	private List<String> fields;

	/**
	 * List of fields involved in the <b>update</b> order. 
	 * updateFieldList is a subset of fields
	 */
	List<String> fieldsToUpdate = new ArrayList<String>();


	/**
	 * The character set of the INPUT data, or an empty string if this property
	 * is not setup
	 */
	public String charSetIn;

	/**
	 * The character set of the OUTPUT data, or an empty string if this property
	 * is not setup
	 */
	public String charSetOut;

	/**
	 * Counter declared as a field inside the class in order to be used and
	 * modified inside lambda expression
	 */
	private int counter = 0;
	
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(ARCGIS_SERVICE);
		descriptors.add(TYPE_OF_FILE);
		descriptors.add(SPATIAL_REFERENCE);
		descriptors.add(QUOTITY);
		descriptors.add(CHARACTER_SET_IN);
		descriptors.add(TYPE_OF_DATA_OPERATION);
		descriptors.add(FIELD_LIST_INSERT);
		descriptors.add(FIELD_LIST_UPDATE);
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
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		
		try {
			final String charSetName = validationContext.getProperty(CHARACTER_SET_IN).getValue();
		
			final String fieldListInsert = validationContext.getProperty(FIELD_LIST_INSERT).getValue();
			fields = parseHeader(fieldListInsert, charSetName);
		
			final String updateFieldsFilename = validationContext.getProperty(FIELD_LIST_UPDATE).getValue();
			if ((fieldsToUpdate != null) && (fieldsToUpdate.size() > 0)) {
				fieldsToUpdate = parseHeader(updateFieldsFilename, charSetName);
			}
			
			return super.customValidate(validationContext);			
		} catch (final Exception e) {
			getLogger().error(ExceptionUtils.getStackTrace(e));
			ValidationResult validationResult = new ValidationResult.Builder().explanation(e.getMessage()).valid(false).build();
			List<ValidationResult> results =  new ArrayList<ValidationResult>();
			results.add(validationResult);
			return results;
		}
	}
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {


		AtomicReference<List<Map<String, String>>> ref_dataParsed = new AtomicReference<List<Map<String, String>>>();
		ref_dataParsed.set(new ArrayList<Map<String, String>>());
		try {
			final String typeOfFile = context.getProperty(TYPE_OF_FILE).getValue();
			if (JSON.equals(typeOfFile)) {
				handleJSONFlow(context, session, ref_dataParsed);
			}

			if (CSV.equals(typeOfFile)) {
				handleCSVFlow(context, session, ref_dataParsed);
			}

			if (ATTRIBUTE.equals(typeOfFile)) {
				throw new RuntimeException("Not implemented yet!");
			}

		} catch (Exception e) {
			getLogger().error(ExceptionUtils.getStackTrace(e));
			session.transfer(session.get(), FAILED);
		}
	}

	/**
	 * Parse the <b><big>JSON</big></b> Flow and save it in an atomic reference.
	 * 
	 * @param context
	 *            the current flow context
	 * @param session
	 *            the current session context
	 * @param reference
	 *            Atomic reference pointed to the data parsed in a list of a Map
	 *            records <b>"row-number"</b>:"list of data values"
	 *            
	 * @throws ProcessException
	 */
	private void handleJSONFlow(final ProcessContext context, final ProcessSession session,
			final AtomicReference<List<Map<String, String>>> ref_dataParsed) throws ProcessException {

		final FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final String charSetName = context.getProperty(CHARACTER_SET_IN).getValue();
		session.read(flowFile, (InputStream inputStream) -> {
			try {
				parseJSONStream(inputStream, charSetName, fieldsToUpdate, ref_dataParsed);
			} catch (final Exception e) {
				getLogger().error(ExceptionUtils.getStackTrace(e));
				session.transfer(session.get(), FAILED);
			}
		});		
	}

	/**
	 * Parse the <b><big>CSV</big></b> Flow and save it in an atomic reference.
	 * 
	 * @param context
	 *            the current flow context
	 * @param session
	 *            the current session context
	 * @param ref_dataParsed
	 *            Atomic reference pointed to the data parsed in list of a Map
	 *            
	 * @throws ProcessException
	 */
	private void handleCSVFlow(final ProcessContext context, final ProcessSession session,
			final AtomicReference<List<Map<String, String>>> ref_dataParsed) throws ProcessException {

		final FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final String charSetName = context.getProperty(CHARACTER_SET_IN).getValue();
		Map<String, String> data = flowFile.getAttributes();
		if (getLogger().isDebugEnabled()) {
			data.keySet().forEach(key -> getLogger().debug(key + " " + data.get(key)));
		}
		
		session.read(flowFile, (InputStream inputStream) -> {
			try {
				parseCSVStream(inputStream, charSetName, ref_dataParsed);
			} catch (final Exception e) {
				getLogger().error(ExceptionUtils.getStackTrace(e));
				session.transfer(session.get(), FAILED);
			}
		});
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("Total number of lines parsed " + String.valueOf(ref_dataParsed.get().size()));
		}

		ArcGISLayerServiceAPI service = context.getProperty(ARCGIS_SERVICE)
				.asControllerService(ArcGISLayerServiceAPI.class);
		boolean headerValid = service.isHeaderValid(fields);
		if (!headerValid) {
			StringBuffer sb = new StringBuffer();
			fields.forEach(column -> sb.append(column).append(","));
			getLogger().error("File header invalid : " + sb.toString());
			session.transfer(flowFile, FAILED);
			return;
		}


		Map<String, Object> settings = new HashMap<String, Object>();
		final String spatialReference = context.getProperty(SPATIAL_REFERENCE).getValue();
		if (spatialReference != null && spatialReference.length() > 0) {
			settings.put(SPATIAL_REFERENCE.getName(), spatialReference);
		}
		final String dataOperation = context.getProperty(TYPE_OF_DATA_OPERATION).getValue();
		getLogger().debug(ArcGISLayerServiceAPI.OPERATION + " = " + dataOperation);
		settings.put(ArcGISLayerServiceAPI.OPERATION, dataOperation);

		
		int quotity = Integer.valueOf(context.getProperty(QUOTITY).getValue());
		final int nb_total_records = ref_dataParsed.get().size();
		getLogger().debug(
				"Processing " + nb_total_records + " records by blocks of " + String.valueOf(quotity) + " elements");

		while (!ref_dataParsed.get().isEmpty()) {
			List<Map<String, String>> processingRecords = new ArrayList<Map<String, String>>();
			for (int i = 0; i < quotity; i++) {
				processingRecords.add(ref_dataParsed.get().remove(0));
				if (ref_dataParsed.get().isEmpty())
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

		session.transfer(flowFile, SUCCESS);
	}


	/**
	 * Parse a fields file and return its content in a collection.
	 * 
	 * @param fieldsFilename the filename containing the fields list in a CSV format
	 * @param charSet the actual character set of this file 
	 * @return a list containing the fields collection
	 * @throws Exception
	 */
	public List<String> parseHeader(final String fieldsFilename, String charSet) throws Exception {

		final InputStream inputStream = new FileInputStream(new File(fieldsFilename));
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charSet));
		StringBuilder sb = FileManager.readLine(reader);
		if (sb == null) {
			throw new Exception("Empty file");
		}
		
		getLogger().debug("parsing the CSV line " + sb.toString());
		return CsvManager.parseLine(sb.toString(), ';');

	}	
	
	/**
	 * Parse a <b>CSV</b> Stream and fill the collection result.
	 * 
	 * @param inputStream
	 *            the inputStream accessing either the flowFile, or the header
	 *            file
	 * @param charSetName
	 *            the current character set
	 * @param ref_dataParsed
	 *            Atomic reference pointed out the parsed content of the CSV file
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public void parseCSVStream(final InputStream inputStream, final String charSetName,
			final AtomicReference<List<Map<String, String>>> ref_dataParsed) throws UnsupportedEncodingException, IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charSetName));
		StringBuilder sb;

		while ((sb = FileManager.readLine(reader)) != null) {
			if (sb.length() > 0) {
				getLogger().debug("parsing the CSV line " + sb.toString());
				List<String> values = CsvManager.parseLine(sb.toString(), ';');
				Map<String, String> record = new HashMap<String, String>();
				if (getLogger().isDebugEnabled()) {
					fields.forEach(fieldName->getLogger().debug(fieldName + " "));				
					values.forEach(value->getLogger().debug(value + " "));				
				}
				fields.forEach(fieldName->record.put(fieldName, values.remove(0)));
				ref_dataParsed.get().add(record);
			}
		}
	}

	/**
	 * Parse a <b>JSON</b> Stream and fill the collection result.
	 * 
	 * @param inputStream
	 *            the inputStream reading the flowFile
	 * @param charSetName
	 *            the stream character set
	 * @param fields lists to be parsed from the fields list 
	 * @param ref_dataParsed
	 *            Atomic reference pointed out the parsed content
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public void parseJSONStream(final InputStream inputStream, final String charSetName, final List<String> fields,
			final AtomicReference<List<Map<String,String>>> ref_dataParsed) throws UnsupportedEncodingException, IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charSetName));
		StringBuilder sbContent = new StringBuilder(), sb;
		while ((sb = FileManager.readLine(reader)) != null) {
			sbContent.append(sb);
		}

		JsonParser parser = new JsonParser();
		JsonElement root = parser.parse(sbContent.toString());
		if ( root.isJsonArray() ) {
			JsonArray content = root.getAsJsonArray();
			content.forEach(element -> {
				final JsonObject record = element.getAsJsonObject();
				Map<String, String> records = new HashMap<String, String>();
				fields.forEach( field -> {
					records.put(field, record.get(field).getAsString());	
				});
				ref_dataParsed.get().add(records);
			});
		}
		
		
		
	}
	
}
