/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import nifi.arcgis.service.arcgis.services.json.ArcGISServicesData;
import nifi.arcgis.service.arcgis.services.json.Layer;

/**
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class DeserializeArcGISServicesTest {

	TestRunner runner;
	
	static {
    	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
	}

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(TestProcessor.class);
    }

    @Test
	public void testExtractFolderServer  () throws IOException {
		String filename = "./target/test-classes/arcgis.rest.services_folders";
		runner.getLogger().debug(new File(filename).getAbsolutePath());
		String json = UtilTest.readJsonFromFile(filename);
		runner.getLogger().debug(json);
		
		Set<String> folderServers = new ArcGISServicesData(runner.getLogger()).getFolderServer(json);
		assertEquals (6, folderServers.size());
		folderServers.forEach(folder -> runner.getLogger().debug(folder.toString()));
		assertTrue (folderServers.contains("ENEDIS"));
	}

    @Test
	public void testExtractFeatureServer  () throws IOException {
		String filename = "./target/test-classes/arcgis.rest.services";
		runner.getLogger().debug(new File(filename).getAbsolutePath());
		String json = UtilTest.readJsonFromFile(filename);
		runner.getLogger().debug(json);
		
		Set<String> featureServers = new ArcGISServicesData(runner.getLogger()).getFeatureServer(json);
		assertEquals (1, featureServers.size());
		assertTrue (featureServers.contains("MyMapService"));
	}

    @Test
 	public void testExtractLayers  () throws IOException {
 		String filename = "./target/test-classes/arcgis.rest.services.MyMapService.FeatureServer";
 		runner.getLogger().debug(new File(filename).getAbsolutePath());
 		String json = UtilTest.readJsonFromFile(filename);
 		runner.getLogger().debug(json);
 		
 		Set<Layer> layers = new ArcGISServicesData(runner.getLogger()).getLayers(json);
 		assertEquals (1, layers.size());
 		Layer [] layer = new Layer[layers.size()];
 		layers.toArray(layer);
 		assertEquals (0, layer[0].id);
 		assertEquals ("geo_db.sde.TOWN", layer[0].name);
 	}

    class Service {
		public String name;
		public String type;

		/** 
		 * Empty constructor
		 */
		public Service() {
		}
		
		/**
		 * @param name name of the service
		 * @param type type of the service. <code>MapServer</code>, <code>FeatureServer</code>
		 */
		public Service(String name, String type) {
			super();
			this.name = name;
			this.type = type;
		}
		
	}
}
