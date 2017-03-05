package nifi.arcgis.processor;

import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

public class MockControllerService extends AbstractControllerService implements nifi.arcgis.service.arcgis.services.ArcGISLayerServiceAPI {

	private boolean headerValid;
	
	public void setHeaderValid(boolean headerValid) {
		this.headerValid = headerValid;
	}
	/**
	 * public construction.
	 */
	public MockControllerService() {
	}
	
	@Override
	public boolean isHeaderValid(List<String> arg0) throws ProcessException {
		return headerValid;
	}
	
	@Override
	public void execute(List<Map<String, String>> arg0, Map<String, Object> arg1) throws ProcessException {
		// TODO Auto-generated method stub
		
	}


	
}
