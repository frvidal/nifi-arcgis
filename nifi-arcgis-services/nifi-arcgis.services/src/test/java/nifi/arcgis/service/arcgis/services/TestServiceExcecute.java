/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.groovy.runtime.metaclass.NewMetaMethod;
import org.junit.Before;
import org.junit.Test;
import org.omg.PortableServer.ServantRetentionPolicyValue;

/**
 * Test the execution of a flow
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class TestServiceExcecute {
	
	public final static boolean testOffline = false;
	
	TestRunner runner;
	
	final String nameControllerService = "arcgisService";
			
	ArcGISLayerService service;
	
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
    public void test_executeService() throws Exception {

    	// test offline
    	if (testOffline) return;
    	
        cleanupDB();

        ValidationResult vr = runner.setProperty(service, ArcGISLayerService.ARCGIS_URL, "http://localhost:6080");
        assertTrue (vr.isValid());
 
        vr = runner.setProperty(service, ArcGISLayerService.FEATURE_SERVER, "MyMapService");
        assertTrue (vr.isValid());
 
        vr = runner.setProperty(service, ArcGISLayerService.FEATURE_SERVER, "geo_db.sde.TOWN");
        assertTrue (vr.isValid());
        
        runner.enableControllerService(service);
        runner.assertNotValid(service);
        
        // name;lattitude;longitude
        // Rouen;49.433333;1.083333
        final Map<String, String> record = new HashMap<String, String>();
        record.put("name", "city-test");
        record.put("lattitude", "50.00");
        record.put("longitude", "50.00");
        final List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        records.add(record);
        final Map<String, String> record2 = new HashMap<String, String>();
        record2.put("name", "city-2");
        record2.put("lattitude", "-3.333333");
        record2.put("longitude", "48.766667");
        records.add(record2);
        
        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put(ArcGISLayerServiceAPI.SPATIAL_REFERENCE, ArcGISLayerServiceAPI.SPATIAL_REFERENCE_WGS84);
        
        ArcGISDataManager dataManager = new ArcGISDataManager(runner.getLogger());
        dataManager.checkConnection("http://localhost:6080", null, "MyMapService", "geo_db.sde.TOWN");
        service.setArcGISDataManager(dataManager);
        service.execute(records, settings);      
        
        List<Map<String, Object>> results = dataManager.search(50, 50, settings, 10000);
        
        assertEquals(2, results.size());
        
        cleanupDB();
    }

    private void cleanupDB() throws Exception {
    	
    	Class.forName("org.postgresql.Driver");
    	Connection connection = null;
    	connection = DriverManager.getConnection(
    	   "jdbc:postgresql://localhost:5432/geo_db","sde", "sde");
    	connection.createStatement().execute("delete from town");
    	connection.close();

    	
    }
    
}
