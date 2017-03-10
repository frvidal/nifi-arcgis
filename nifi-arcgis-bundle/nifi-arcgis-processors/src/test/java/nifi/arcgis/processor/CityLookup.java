package nifi.arcgis.processor;

import com.maxmind.geoip.*;
import java.io.IOException;

/* sample of how to use the GeoIP Java API with GeoIP City database */
/* Usage: java CityLookup 64.4.4.4 */

class CityLookup {
    public static void main(String...args) {
	try {
	    LookupService cl = new LookupService("/Users/frvidal/GeoLiteCity.dat",
					LookupService.GEOIP_MEMORY_CACHE );
            Location l1 = cl.getLocation("77.204.77.145");
            System.out.println(l1);
            Location l2 = cl.getLocation("77.204.77.145");
            System.out.println(l2);
            	    System.out.println("countryCode: " + l2.countryCode +
                               "\n countryName: " + l2.countryName +
                               "\n region: " + l2.region +
                               "\n regionName: " + regionName.regionNameByCode(l2.countryCode, l2.region) +
                               "\n city: " + l2.city +
                               "\n postalCode: " + l2.postalCode +
                               "\n latitude: " + l2.latitude +
                               "\n longitude: " + l2.longitude +
                               "\n distance: " + l2.distance(l1) +
                               "\n distance: " + l1.distance(l2) +
 			       "\n metro code: " + l2.metro_code +
 			       "\n area code: " + l2.area_code +
                               "\n timezone: " + timeZone.timeZoneByCountryAndRegion(l2.countryCode, l2.region));

	    cl.close();
	}
	catch (IOException e) {
	    System.out.println("IO Exception");
	}
    }
}