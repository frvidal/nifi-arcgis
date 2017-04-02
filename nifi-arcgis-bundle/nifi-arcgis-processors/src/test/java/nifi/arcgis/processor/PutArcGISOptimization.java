/**
 * 
 */
package nifi.arcgis.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_INSERT;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.OPERATION_UPDATE;
import static nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI.UPDATE_FIELD_LIST;



/**
 * Testing a JSON file entry.
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class PutArcGISOptimization {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
	}

	private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(PutArcGIS.class);
        testRunner.setProperty(PutArcGIS.TYPE_OF_FILE, "JSON");
        testRunner.setProperty(PutArcGIS.ARCGIS_SERVICE, "arcgis-service");
        testRunner.setProperty(PutArcGIS.QUOTITY, "100");
        		
        MockControllerService service = new MockControllerService();
        testRunner.addControllerService("arcgis-service", service);
        testRunner.enableControllerService(service);
    }

    /**
     * Test of optimizationDataForUpdate for an inert operation mode
     * @throws Exception
     */
    @Test
    public void testProcessorOptimizeRecords_operationInsert() throws Exception {
        PutArcGIS putArcGIS = (PutArcGIS) testRunner.getProcessor();
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put(OPERATION, OPERATION_INSERT);
        List<Map<String, String>> list = putArcGIS.optimizationDataForUpdate(new ArrayList<Map<String, String>>(), settings);
        assertNull(list);	
    }

    /**
     * Test of optimizationDataForUpdate for an inert update fields list 
     * @throws Exception
     */
    @Test
    public void testProcessorOptimizeRecordsInvalidUpdateFieldList() throws Exception {
    	
        PutArcGIS putArcGIS = (PutArcGIS) testRunner.getProcessor();
        Map<String, Object> settings = new HashMap<String, Object>();
        List<String> updateFieldsList = new ArrayList<String>();
        updateFieldsList.add(0, "hit");
        settings.put(OPERATION, OPERATION_UPDATE);
        settings.put(UPDATE_FIELD_LIST, updateFieldsList);
        List<Map<String, String>> list = putArcGIS.optimizationDataForUpdate(new ArrayList<Map<String, String>>(), settings);
        assertNull(list);
        updateFieldsList.remove(0);
        updateFieldsList.add(0, "+hit");
        list = putArcGIS.optimizationDataForUpdate(new ArrayList<Map<String, String>>(), settings);
        assertNotNull(list);
        updateFieldsList.add(1, "ko");
        list = putArcGIS.optimizationDataForUpdate(new ArrayList<Map<String, String>>(), settings);
        assertNull(list);
    }
    
    @Test
    public void testProcessorOptimizeRecords() throws Exception {
    	
    	testRunner.getLogger().debug(this.getClass().getClassLoader().getResource(".").getFile());
    	testRunner.setProperty(PutArcGIS.TYPE_OF_FILE, "JSON");
        testRunner.setProperty(PutArcGIS.FIELD_LIST_INSERT, this.getClass().getClassLoader().getResource(".").getFile() + "/header-ok");
        testRunner.setProperty(PutArcGIS.FIELD_LIST_UPDATE, this.getClass().getClassLoader().getResource(".").getFile() + "/header-update-single");

        testRunner.setProperty(PutArcGIS.TYPE_OF_DATA_OPERATION, OPERATION_UPDATE);

        // the ProcessorService is supposed to validate the column lists
        // We mock this behavior in this MockControllerService
        testRunner.getControllerService("arcgis-service", MockControllerService.class).setHeaderValid(true);
        File f = new File(this.getClass().getClassLoader().getResource("./log-for-optimization.log").getFile());
        assertTrue (f.exists());
        final InputStream content = new FileInputStream(this.getClass().getClassLoader().getResource("./log-for-optimization.log").getFile());
        assertNotNull(content);
        
        testRunner.assertValid();
        PutArcGIS putArcGIS = (PutArcGIS) testRunner.getProcessor();
        assertNotNull("putArcGIS", putArcGIS);

        assertNotNull("fieldsToUpdate", putArcGIS.fieldsToUpdate);
        List<String> list = new ArrayList<String>();
        list.add(0,  "+hit");
        assertEquals("fieldsToUpdate", list, putArcGIS.fieldsToUpdate);

        // Add the content to the runner
    	testRunner.enqueue(content);
        
    	// Launch the runner
    	testRunner.run(1);
        testRunner.assertQueueEmpty();
    	testRunner.assertValid();
    	
    	List<Map<String, String>> records = testRunner.getControllerService("arcgis-service", MockControllerService.class).getExecuteArg0();
    	assertEquals(5, records.size());
    }
 
}
