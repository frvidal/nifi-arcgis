package nifi.arcgis.service.arcgis.services;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Test {

	@org.junit.Test
	public void test() throws Exception {
		
		Exception e = new Exception ("test");
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));

		System.out.println(sw.toString());
	}
}
