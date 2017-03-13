package nifi.arcgis.processor;

import java.lang.reflect.Executable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Class of test for testing java purpose
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 *
 */
public class Test {

	@org.junit.Test
	public void test() throws Exception {
		String s = "PlouÃ©nan";        
	    byte[] b = s.getBytes("ISO-8859-1");
	    String t = new String(b, "UTF-8");
	    System.out.println(t);
	 
    	Class.forName("org.postgresql.Driver");
    	Connection connection = null;
    	connection = DriverManager.getConnection(
    	   "jdbc:postgresql://localhost:5432/geo_db","sde", "sde");
    	
    	PreparedStatement stmt = connection.prepareStatement("select NAME from TOWN where name like 'Plou%'", 
    			ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
    	ResultSet rs = stmt.executeQuery();
    	while (rs.next()) {
    		String chaine = rs.getString(1);
    		System.out.println(chaine);
    	}
    	connection.close();
	    
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
