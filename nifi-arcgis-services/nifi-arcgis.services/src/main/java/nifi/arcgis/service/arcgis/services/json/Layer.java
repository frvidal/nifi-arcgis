package nifi.arcgis.service.arcgis.services.json;

public class Layer {
	
	public int id;
	
	public String name;

	/**
	 * @param id layer like <code>0</code>
	 * @param name name
	 */
	public Layer(int id, String name) {
		super();
		this.id = id;
		this.name = name;
	}
	
	
}
