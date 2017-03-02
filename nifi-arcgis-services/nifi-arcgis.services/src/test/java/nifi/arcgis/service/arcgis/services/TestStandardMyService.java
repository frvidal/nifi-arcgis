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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import nifi.arcgis.service.arcgis.services.json.NetworkUtility;

public class TestStandardMyService {

	TestRunner runner;
	
	final String nameControllerService = "arcgisService";
			
	ControllerService service;
	
	static {
    	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
	}
	
    @Before
    public void init() {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        service = new ArcGISLayerService();
        try {
        	runner.addControllerService(nameControllerService, service);
        } catch (InitializationException ie) {
        	fail(ie.getMessage());
        }
    }

    @Test
    public void testSetPropertyBadUrl() throws InitializationException {

    	assertFalse("onPropertyModified has not been called", ((ArcGISLayerService) service).isOpmCalled());

        final String BAD_URL = "a-bad-url";
        ValidationResult vr = runner.setProperty(service, ArcGISLayerService.ARCGIS_URL, BAD_URL);
        assertFalse (vr.isValid());
 
        // In version 1.1.1, onPropertyModified is not invoked
        // We leave assertFalse in  order to be notified of the fix 
        assertFalse("[FIX OCCURS IN NIFI] onPropertyModified has been called", ((ArcGISLayerService) service).isOpmCalled());
       // Test of the connection is added directly here
        runner.getControllerService(nameControllerService).onPropertyModified(ArcGISLayerService.ARCGIS_URL, "", "BAD_URL");
 
        runner.enableControllerService(service);
        runner.assertNotValid(service);
    }

    @Test
    public void testSetPropertyGoodUrlServerDown() throws InitializationException, IOException {
   	
        assertFalse("onPropertyModified has not been called", ((ArcGISLayerService) service).isOpmCalled());

        final String GOOD_URL = "http://arcgis-server-ko:6080/arcgis/rest/services/MyMapService/FeatureServer/0";
        ValidationResult vr = runner.setProperty(service, ArcGISLayerService.ARCGIS_URL, GOOD_URL);
        assertTrue (vr.isValid());
        
        assertFalse( NetworkUtility.isReachable(GOOD_URL));
    }
    
}
