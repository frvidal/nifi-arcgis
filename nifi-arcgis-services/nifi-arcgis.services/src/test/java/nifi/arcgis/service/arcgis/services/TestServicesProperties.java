/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import static org.junit.Assert.*;


import static nifi.arcgis.service.arcgis.services.ArcGISLayerService.*;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.ExceptionList;

/**
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class TestServicesProperties {

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

	/**
	 * @throws Exception
	 */
	@Test
	public void testProperties() throws Exception {
		ValidationResult vr;
		vr = runner.setProperty(service, ARCGIS_URL, "http://localhost:6080");
		if (!vr.isValid()) runner.getLogger().debug(vr.getExplanation());
		assertTrue(vr.isValid());
		
		vr = runner.setProperty(service, FEATURE_SERVER, "city");
		assertTrue(vr.isValid());
		vr = runner.setProperty(service, LAYER_NAME, "geo_db.sde.CITY");
		assertTrue(vr.isValid());
		
		// 
		runner.assertValid(service);
	}
}
