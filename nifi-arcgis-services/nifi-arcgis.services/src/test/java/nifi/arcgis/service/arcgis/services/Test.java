package nifi.arcgis.service.arcgis.services;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class Test {

	final Map<String, Object> record = new HashMap<String, Object>();

	@org.junit.Test
	public void test() throws Exception {

		record.put("TEST", new Integer(2));

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("TEST", new Integer(1));
		map.compute("TEST", biTest);

	}

	public BiFunction<? super String, ? super Object, ? extends Object> biTest = (x,
			y) -> (new Integer((Integer) record.get(x) + (Integer) y));

	@org.junit.Test
	public void breakAndContinue() {
		for (int i = 0; i < 10; i++) {
			if (i == 5)
				break;
			System.out.println(i);
		}
		for (int i = 0; i < 10; i++) {
			if (i == 5)
				continue;
			System.out.println(i);
		}
	}

}
