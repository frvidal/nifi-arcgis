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
		getLogger().debug("execution in MockControllerService");
		this.last_arg0 = arg0;
		this.last_arg1 = arg1;
	}

	List<Map<String, String>> last_arg0;
	Map<String, Object> last_arg1;
	
	/**
	 * @return the first argument passed to the execute method.
	 */
	public List<Map<String, String>> getExecuteArg0() {
		return last_arg0;
	}
	
	/**
	 * @return the second argument passed to the execute method.
	 */
	public Map<String, Object> getExecuteArg1() {
		return last_arg1;
	}

}
