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

import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"put"})
@CapabilityDescription("Data management accessor for ArcGIS server.")
public interface ArcGISLayerServiceAPI extends ControllerService {

	// Type of spatial reference
	public final static String SPATIAL_REFERENCE = "SPATIAL_REFERENCE";
	public final static String SPATIAL_REFERENCE_WGS84 = "Wgs84";
	public final static String SPATIAL_REFERENCE_WEBMERCATOR = "WebMercator";

	// Type of operation executed on the ArcGIS server
	public final static String OPERATION = "Type operation";
	public final static String OPERATION_INSERT = "ADD";
	public final static String OPERATION_UPDATE = "EDIT";
	public final static String OPERATION_UPDATE_OR_INSERT = "ADD OR EDIT";
	
	public final static String TYPE_OF_QUERY = "QUERY";
	public final static String TYPE_OF_QUERY_GEO = "GEO";
	
	/**
	 * This property represents the radius of the search-circle around a point in a geo-query search 
	 */
	public final static String RADIUS = "RADIUS";
	
	/**
	 * This property represents the list of columns involved in the updates operation
	 */
	public final static String UPDATE_FIELD_LIST = "U_F_L";
	
	/**
	 * <p> 
	 * Validate the header for <b>CSV file</b> : The header must contain the list of column names for the targeted featureService.
	 * </p>
	 * @param header the header parsed from the CSV in a list of string
	 * @return <code>TRUE</code> if this header match the columns list available for this feature serve, <code>FALSE</code> otherwise
     * @throws ProcessException thrown if any problems occurs during execution
	 */
    public boolean isHeaderValid(List<String> header) throws ProcessException;

    /**
     * Process a record
     * @param list of records in a Map Format (Key, Value)
     * @param settings data settings (such as spatial reference)
     * @throws ProcessException thrown if any problems occurs during execution
     */
    public void execute(List<Map<String, String>> record, final Map<String, Object> settings)  throws ProcessException;
    
    
}
