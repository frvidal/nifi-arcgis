/**
 * 
 */
package nifi.arcgis.processor.processors.arcgis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class UtilFile {

	/**
	 * @param inputStream inputStream of a flowFile
	 * @return content of the inputStream
	 * @throws IOException Exception occurs while reading the inputStream
	 */
	public static StringBuilder read(final InputStream inputStream) throws IOException {
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
	      StringBuilder content = read(rd);
	      return content;
	    } finally {
	      inputStream.close();
	    }
	  }

	/**
	 * @param reader
	 * @return the content of the reader
	 * @throws IOException
	 */
	private static StringBuilder read(Reader reader) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = reader.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb;
	  }

}
