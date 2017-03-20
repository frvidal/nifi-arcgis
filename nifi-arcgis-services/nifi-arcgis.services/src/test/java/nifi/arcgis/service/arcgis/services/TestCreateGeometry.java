/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import junit.framework.AssertionFailedError;

/**
 * Testing the creation of the geometry.
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class TestCreateGeometry {

	TestRunner runner;
	ArcGISDataManager dataManager;
	Map<String, String> records;
	Map<String, Object> settings;

	@Before
	public void init() {
		runner = TestRunners.newTestRunner(TestProcessor.class);
		dataManager = new ArcGISDataManager(runner.getLogger());
		records = new HashMap<String, String>();
		settings = new HashMap<String, Object>();		
	}

	@Test
	public void testCreationIncompleteData_y_leak() {
		records.put("n/a", "test");
		records.put("x", "test");
		try {
			dataManager.createPoint(records, settings);
			Assert.fail("Should send an exception");
		} catch (Exception e) {
		}
	}

	@Test
	public void testCreationIncompleteData_x_leak() {
		records.put("n/a", "test");
		records.put("y", "test");
		try {
			dataManager.createPoint(records, settings);
			Assert.fail("Should send an exception");
		} catch (Exception e) {
		}
	}

	@Test
	public void testCreationIncompleteData_x_y_ok() {
		records.put("n/a", "test");
		records.put("x", "1");
		records.put("y", "2");
		try {
			dataManager.createPoint(records, settings);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Should NOT send an exception");
		}
	}

	@Test
	public void testCreationIncompleteData_longitude() {
		records.put("n/a", "test");
		records.put("longitude", "test");
		try {
			dataManager.createPoint(records, settings);
			Assert.fail("Should send an exception");
		} catch (Exception e) {
		}
	}

	@Test
	public void testCreationIncompleteData_lattitude() {
		records.put("n/a", "test");
		records.put("lattitude", "test");
		try {
			dataManager.createPoint(records, settings);
			Assert.fail("Should send an exception");
		} catch (Exception e) {
		}
	}

	@Test
	public void testCreationIncompleteData_lattitude_longitude_ok() {
		records.put("n/a", "test");
		records.put("latitude", "1");
		records.put("longitude", "2");
		try {
			dataManager.createPoint(records, settings);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Should NOT send an exception");
		}
	}
}
