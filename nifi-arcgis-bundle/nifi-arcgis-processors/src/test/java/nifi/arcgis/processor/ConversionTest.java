package nifi.arcgis.processor;

import static org.junit.Assert.assertEquals;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;

public class ConversionTest {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
	}

	private TestRunner testRunner;

	@Before
	public void init() throws Exception {
		testRunner = TestRunners.newTestRunner(PutArcGIS.class);
	}

	@org.junit.Test
	public void test() throws Exception {
		String s = "RhÃ´ne-Alpes"; 
	    byte[] b = s.getBytes("ISO-8859-1");
	    String t = new String(b, "UTF-8");
	    testRunner.getLogger().debug(t);	    
	    assertEquals("Rhône-Alpes", t);
	}

	@org.junit.Test
	public void testConvert() {
		PutArcGIS putArcGIS = new PutArcGIS();
		putArcGIS.charSetIn = "ISO-8859-1";
		putArcGIS.charSetOut = "UTF-8";
		String in = "RhÃ´ne-Alpes";
		testRunner.getLogger().debug("from " + in);
		String encoded = putArcGIS.convert(in);
		assertEquals("Rhône-Alpes", encoded);	
		testRunner.getLogger().debug("to " + encoded);	
	}

}
