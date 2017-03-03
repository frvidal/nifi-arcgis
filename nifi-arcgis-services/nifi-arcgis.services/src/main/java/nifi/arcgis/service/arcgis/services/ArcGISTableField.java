/**
 * 
 */
package nifi.arcgis.service.arcgis.services;

import com.esri.arcgisruntime.data.Field.Type;

/**
 * POJO class representing a column inside a featureTable in ArcGIS server.
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class ArcGISTableField {

	/**
	 * simple Name.
	 */
	String name;
	
	/**
	 * Type as OID, INTEGER, DOUBLE, TEXT
	 */
	Type type;

	
	/**
	 * Public constructor
	 * @param name
	 * @param type
	 */
	public ArcGISTableField(String name, Type type) {
		super();
		this.name = name;
		this.type = type;
	}

}
