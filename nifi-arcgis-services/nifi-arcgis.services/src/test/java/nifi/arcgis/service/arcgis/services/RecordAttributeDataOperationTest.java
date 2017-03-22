/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Testing the feature <code>attributes.computeIfPresent</code> with the participation of RecordAttributeDataOperation.
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class RecordAttributeDataOperationTest {

	@org.junit.Test
	public void addInteger() throws Exception {
		Map<String, String> record = new HashMap<String, String>();
		record.put("string", "new");
		record.put("+int", "4");
		
		RecordAttributeDataOperation dataOperation = new RecordAttributeDataOperation (record);
		
		Map<String, Object> db_record = new HashMap<String, Object>();
		db_record.put("OID", "OID");
		db_record.put("string", "old");
		db_record.put("int", new Integer(1));
		
		final BiFunction<String, ? super Object, ? extends Object> fctDataOperation = 
				ArcGISDataManager.getBiFunction("+int", dataOperation, new Integer(4));
		
		
		db_record.computeIfPresent("int", fctDataOperation);
		
		assertEquals(new Integer(5), db_record.get("int"));
	}


	@org.junit.Test
	public void subtractInteger() throws Exception {
		Map<String, String> record = new HashMap<String, String>();
		record.put("string", "new");
		record.put("-int", "4");
		
		RecordAttributeDataOperation dataOperation = new RecordAttributeDataOperation (record);
		
		Map<String, Object> db_record = new HashMap<String, Object>();
		db_record.put("OID", "OID");
		db_record.put("string", "old");
		db_record.put("int", new Integer(1));
		
		final BiFunction<String, ? super Object, ? extends Object> fctDataOperation = 
				ArcGISDataManager.getBiFunction("-int", dataOperation, new Integer(4));
		
		
		db_record.computeIfPresent("int", fctDataOperation);
		
		assertEquals(new Integer(-3), db_record.get("int"));
	}

	@org.junit.Test
	public void setInteger() throws Exception {
		Map<String, String> record = new HashMap<String, String>();
		record.put("string", "new");
		record.put("int", "4");
		
		RecordAttributeDataOperation dataOperation = new RecordAttributeDataOperation (record);
		
		Map<String, Object> db_record = new HashMap<String, Object>();
		db_record.put("OID", "OID");
		db_record.put("string", "old");
		db_record.put("int", new Integer(1));
		
		final BiFunction<String, ? super Object, ? extends Object> fctDataOperation = 
				ArcGISDataManager.getBiFunction("int", dataOperation, new Integer(4));
		
		
		db_record.computeIfPresent("int", fctDataOperation);
		
		assertEquals(new Integer(4), db_record.get("int"));
	}
}
