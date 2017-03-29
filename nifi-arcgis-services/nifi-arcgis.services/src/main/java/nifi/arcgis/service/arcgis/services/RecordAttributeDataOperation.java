/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Repository of BiFunctions for data manipulation.
 * <br/>The purpose of this class is to manipulate date in the Map
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class RecordAttributeDataOperation {

	/**
	 * Record treated
	 */
	Map<String, String> record;
	
	
	/**
	 * Construction of this class
	 * @param record record in progress.
	 */
	public RecordAttributeDataOperation(final Map<String, String> record) {
		super();
		this.record = record;
	}
	
	/**
	 * Add the passed numeric in the record to the numeric stored into the database.
	 * <br/><i>We add the operator <code>+</code> which is prefixing the field name in the record map</i>
	 * <br/>We try to produce something like <code>set A = A + :1</code>
	 */
	public BiFunction<String, ? super Object, Object> addInteger = (k, v) -> (Integer) v + Integer.valueOf(record.get(k));
	

	/**
	 * Subtract the passed numeric in the record to the numeric stored into the database
	 * <br/><i>We add the operator <code>-</code> which is prefixing the field name in the record map</i>
	 * <br/>We try to produce something like <code>set A = A -:1</code>
	 */
	public BiFunction<String, ? super Object, Object> subtractInteger = (k, v) -> (Integer) v - Integer.valueOf(record.get(k));

	/**
	 * Replace the old numeric value by the new numeric value into the database.
	 * <br/>We try to produce a simple <code>set A = :1</code>
	 */
	public BiFunction<String, ? super Object, Object> setInteger = (k, v) -> Integer.valueOf(record.get(k));

	/**
	 * Add the passed numeric in the record to the numeric stored into the database
	 * <br/><i>We add the operator <code>+</code> which is prefixing the field name in the record map</i>
	 * <br/>We try to produce something like <code>set A = A + :1</code>
	 */
	public BiFunction<String, ? super Object, Object> addDouble = (k, v) -> (Double) v + Double.valueOf(record.get(k)) ;

	/**
	 * Subtract the passed numeric in the record to the numeric stored into the database
	 * <br/><i>We add the operator <code>-</code> which is prefixing the field name in the record map</i>
	 * <br/>We try to produce something like <code>set A = A -:1</code>
	 */
	public BiFunction<String, ? super Object, Object> subtractDouble = (k, v) -> (Double) v - Double.valueOf(record.get(k));

	/**
	 * Replace the old numeric value by the new numeric value into the database.
	 * <br/>We try to produce a simple <code>set A = :1</code>
	 */
	public BiFunction<String, ? super Object, Object> setDouble = (k, v) -> Double.valueOf(record.get(k));

	/**
	 * Replace the old numeric value by the new numeric value into the database.
	 * <br/>We try to produce a simple <code>set A = :1</code>
	 */
	public BiFunction<String, ? super Object, Object> setString = (x, y) ->  y;

}
