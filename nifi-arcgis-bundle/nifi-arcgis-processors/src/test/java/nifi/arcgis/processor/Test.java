package nifi.arcgis.processor;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.apache.nifi.stream.io.SynchronizedByteCountingOutputStream;

import com.maxmind.geoip.LookupService;

/**
 * Class of test for testing java purpose
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class Test {

	@org.junit.Test
	public void test() throws Exception {
		String s = "RhÃ´ne-Alpes";        
	    byte[] b = s.getBytes("ISO-8859-1");
	    String t = new String(b, "UTF-8");
	    System.out.println(t);
	    
		/*
		
		Charset utf8charset = Charset.forName("UTF-8");
		Charset iso88591charset = Charset.forName("ISO-8859-1");

		ByteBuffer inputBuffer = ByteBuffer.wrap(new byte[]{(byte)0xC3, (byte)0xA2});

		// decode UTF-8
		CharBuffer data = utf8charset.decode(inputBuffer);

		// encode ISO-8559-1
		ByteBuffer outputBuffer = iso88591charset.encode(data);
		byte[] outputData = outputBuffer.array();
		
		System.out.println(outputData);
		
		*/
	}
}
