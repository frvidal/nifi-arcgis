/**
 * 
 */
package nifi.arcgis.processor.utility;

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
public class FileManager {
	
	private static final char LN = System.getProperty ("line.separator").charAt(0);
	
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
	 * Read a simple line from the reader.
	 * @param reader 
	 * @return a StringBuilder containing one line from the reader, or <code>null</code> if we reached the end of the file
	 * @throws IOException 
	 */
	public static StringBuilder readLine(Reader reader) throws IOException {
	
		// Have we reached the end of the file ?
		boolean eof = true;
	    
		StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = reader.read()) != -1) {
	    	
	    	eof = false;
	    	
	    	// End of line
	    	if (cp == LN) break;
	    	
	    	sb.append((char) cp);
	    }
	    return (eof ? null : sb);
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
