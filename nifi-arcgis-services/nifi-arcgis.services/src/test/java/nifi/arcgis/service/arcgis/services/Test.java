package nifi.arcgis.service.arcgis.services;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Test {

	volatile int i = 0;
	Object o = new Object();
	
	@org.junit.Test
	public void test() throws Exception {
		
		
		System.out.println(i++);
		t1.start();
		synchronized(o) {
			o.wait();
		}
		System.out.println(i++);

	}
	
	Thread t1 = new Thread(() -> {
		System.out.println("go " + i++);
		try {
			Thread.sleep(5000);
		} catch (final Exception e) {
			e.printStackTrace();
		}
		synchronized (o) {
			o.notify();
		}
	});
}
