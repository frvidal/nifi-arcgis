/**
 * 
 */
package nifi.arcgis.processor;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI;

/**
 * Testing a JSON file entry.
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class PutArcGIS_JSON {

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

    @Test
    public void testProcessorSimpleCSV_test_HEADERFAILED() throws Exception {
    	
    	testRunner.getLogger().debug(this.getClass().getClassLoader().getResource(".").getFile());
        testRunner.setProperty(PutArcGIS.FIELD_LIST_INSERT, this.getClass().getClassLoader().getResource(".").getFile() + "/header-ok");
        testRunner.setProperty(PutArcGIS.FIELD_LIST_UPDATE, this.getClass().getClassLoader().getResource(".").getFile() + "/header-ok");

        testRunner.setProperty(PutArcGIS.TYPE_OF_DATA_OPERATION, ArcGISLayerServiceAPI.OPERATION_INSERT);

        // the ProcessorService is supposed to validate the column lists
        // We mock this behavior in this MockControllerService
        testRunner.getControllerService("arcgis-service", MockControllerService.class).setHeaderValid(false);
        File f = new File(this.getClass().getClassLoader().getResource(".").getFile() + "/1400.log");
        assertEquals(true, f.exists());
        System.out.println(f.getAbsolutePath());
        final InputStream content = new FileInputStream(this.getClass().getClassLoader().getResource(".").getFile() + "/1400.log");
    
    	// Add the content to the runner
    	testRunner.enqueue(content);
    	 
    	// Launch the runner
    	testRunner.run(1);
        testRunner.assertQueueEmpty();
    	testRunner.assertValid();
    	
 
    }

}
