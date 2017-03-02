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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import nifi.arcgis.processor.PutArcGIS;


public class PutArcGISTest {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
	}

	private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(PutArcGIS.class);
        testRunner.setProperty(PutArcGIS.TYPE_OF_FILE, "CSV");
        testRunner.setProperty(PutArcGIS.ARCGIS_SERVICE, "arcgis-service");
		
        MockControllerService service = new MockControllerService();
        testRunner.addControllerService("arcgis-service", service);
        testRunner.enableControllerService(service);
        
        testRunner.run();
    }

    @Test
    public void testProcessorSimpleCSV() throws Exception {
    	
    	final InputStream content = new FileInputStream("./target/test-classes/test_simple_une_ligne_Paris.csv");
    
    	// Add the content to the runner
    	testRunner.enqueue(content);
    	
    	// Launch the runner
    	testRunner.run(1);
    	
    	testRunner.assertQueueEmpty();
    	
    	
    }

}
