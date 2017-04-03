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

	/**
	 * Threshold of display.
	 */
	public final static int THRESHOLD = 1000;
	
	/**
	 * Starting & ending time for the trigger event
	 */
	long start, end;
	
	/**
	 * Previous number of lines treated. 
	 * It's a working instance.
	 */
	float former_total_number_of_lines_treated = 0;

	/**
	 * Total number of lines treated
	 */
	float total_number_of_lines_treated = 0;
	
	/**
	 * Total processing time 
	 */
	float total_duration = 0;

	final ComponentLog logger;
	
	/**
	 * Construction.
	 * @param logger actual logger of the processor
	 */
	Watch(final ComponentLog logger) {
		this.logger = logger;
	}
	
	/**
	 * Get and keep the starting time.
	 */
	public void start() {
		start = System.currentTimeMillis();
	}
	
	/**
	 * Get and keep the ending time.
	 */
	public void end() {
		end = System.currentTimeMillis();
	}
	
	/**
	 * Display the global performance of the data-processing.
	 * @param number_of_lines_treated number of lines treated (without underscore)
	 */
	public void display(long number_of_lines_treated) {
		long duration = end - start;
		
		total_number_of_lines_treated +=  number_of_lines_treated;
		total_duration +=  duration;
		
		if ( total_number_of_lines_treated > (former_total_number_of_lines_treated+THRESHOLD) ) {
			float mean = ((float) ((total_number_of_lines_treated*THRESHOLD)/total_duration));
			logger.info("Speed for " + String.valueOf(THRESHOLD) + " : " + String.valueOf( mean));
			former_total_number_of_lines_treated = total_number_of_lines_treated;
		}
	}
	
}
