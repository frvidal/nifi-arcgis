/**
 * 
 */
package nifi.arcgis.processor;

import org.apache.nifi.logging.ComponentLog;

/**
 * Personal "Breitling" to follow up the performance of the data processing
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL
 */
public class Watch {

	long start, end;
	
	long former_total_number_of_lines_treated = 0;
	long total_number_of_lines_treated = 0;
	long total_duration = 0;

	final ComponentLog logger;
	
	/**
	 * Construction.
	 * @param logger actual logger of the processor
	 */
	Watch(final ComponentLog logger) {
		this.logger = logger;
	}
	
	/**
	 * get and keep the starting time.
	 */
	public void start() {
		start = System.currentTimeMillis();
	}
	
	/**
	 * get and keep the ending time.
	 */
	public void end() {
		end = System.currentTimeMillis();
	}
	
	/**
	 * Display the global performance of the data-processing.
	 */
	public void display(long number_of_lines_treated) {
		long duration = end - start;
		
		total_number_of_lines_treated +=  number_of_lines_treated;
		total_duration +=  duration;
		logger.info(String.valueOf(number_of_lines_treated));
		logger.info(String.valueOf(total_number_of_lines_treated));
		logger.info(String.valueOf(duration));
		logger.info(String.valueOf((float) (total_number_of_lines_treated/total_duration)));
		logger.info(String.valueOf(String.valueOf(former_total_number_of_lines_treated+1000)));
		
		if ( total_number_of_lines_treated > (former_total_number_of_lines_treated+1000) ) {
			float mean = ((float) (total_number_of_lines_treated/total_duration)) * 1000;
			logger.info("Speed " + String.valueOf( mean));
			former_total_number_of_lines_treated = total_number_of_lines_treated;
		}
}
	
}
