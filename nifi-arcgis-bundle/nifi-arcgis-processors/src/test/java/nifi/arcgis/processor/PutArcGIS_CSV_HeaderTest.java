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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import junit.framework.AssertionFailedError;
import nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI;

import static org.junit.Assert.assertEquals;

public class PutArcGIS_CSV_HeaderTest {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
	}

	private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(PutArcGIS.class);
        testRunner.setProperty(PutArcGIS.TYPE_OF_FILE, "CSV");
        testRunner.setProperty(PutArcGIS.ARCGIS_SERVICE, "arcgis-service");
        testRunner.setProperty(PutArcGIS.QUOTITY, "100");
        		
        MockControllerService service = new MockControllerService();
        testRunner.addControllerService("arcgis-service", service);
        testRunner.enableControllerService(service);
    }

    @Test
    public void testProcessorSimpleCSV_test_HEADERFAILED() throws Exception {
    	
    	testRunner.getLogger().debug(this.getClass().getClassLoader().getResource(".").getFile());
        testRunner.setProperty(PutArcGIS.FIELD_LIST_INSERT, this.getClass().getClassLoader().getResource(".").getFile() + "/header-ko");

        testRunner.setProperty(PutArcGIS.TYPE_OF_DATA_OPERATION, ArcGISLayerServiceAPI.OPERATION_INSERT);

        // the ProcessorService is supposed to validate the column lists
        // We mock this behavior in this MockControllerService
        testRunner.getControllerService("arcgis-service", MockControllerService.class).setHeaderValid(false);

    	final InputStream content = new FileInputStream("./target/test-classes/test_simple_une_ligne_Rouen_Noheader_OK.csv");
    
    	// Add the content to the runner
    	testRunner.enqueue(content);
    	 
    	// Launch the runner
    	testRunner.run(1);
        testRunner.assertQueueEmpty();
    	testRunner.assertValid();
    	
    	List<MockFlowFile> failedFiles = testRunner.getFlowFilesForRelationship(PutArcGIS.FAILED);
    	assertEquals(1, failedFiles.size());

    }

    @Test
    public void testProcessorSimpleCSV_test_HEADERPASSED() throws Exception {
    	
    	testRunner.getLogger().debug(this.getClass().getClassLoader().getResource("./header-ok").getFile());
    	
    	testRunner.getControllerService("arcgis-service", MockControllerService.class).setHeaderValid(true);
    	
        testRunner.setProperty(PutArcGIS.FIELD_LIST_INSERT, this.getClass().getClassLoader().getResource("./header-ok").getFile());
        testRunner.setProperty(PutArcGIS.TYPE_OF_DATA_OPERATION, ArcGISLayerServiceAPI.OPERATION_INSERT);

        final InputStream content = new FileInputStream("./target/test-classes/test_simple_une_ligne_Rouen_header_OK.csv");
        
    	// Add the content to the runner
    	testRunner.enqueue(content);
    	
    	// Launch the runner
    	testRunner.run(1);    	
        testRunner.assertQueueEmpty();
    	testRunner.assertValid();

    	List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PutArcGIS.SUCCESS);
    	assertEquals(1, successFiles.size());
    	
    	List<Map<String, String>> parsedContent = testRunner.getControllerService("arcgis-service", MockControllerService.class).getExecuteArg0();
    	Map<String, String> lineHeader = parsedContent.get(0);  
    	System.out.println(lineHeader);
    	assertEquals("Rouen", lineHeader.get("name"));
    	assertEquals("49.433333", lineHeader.get("latitude"));
    	assertEquals("1.083333", lineHeader.get("longitude"));
    }

}