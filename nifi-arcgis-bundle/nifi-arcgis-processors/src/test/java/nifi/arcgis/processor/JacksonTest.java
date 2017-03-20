package nifi.arcgis.processor;

import javax.swing.text.html.parser.Element;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JacksonTest {

	final String simpleJSON = "[{\"test\":\"A\", \"inutile\":\"n/a\"},{\"test\":\"B\"}]";
	
	@org.junit.Test
	public void test_simpleJSON() {
		JsonParser parser = new JsonParser();
		JsonElement root = parser.parse(simpleJSON);
		if ( root.isJsonArray() ) {
			JsonArray content = root.getAsJsonArray();
			content.forEach(element -> {
				JsonObject o = element.getAsJsonObject();
				System.out.println((o.get("test").getAsString()));	
			});
		}
	}
}
