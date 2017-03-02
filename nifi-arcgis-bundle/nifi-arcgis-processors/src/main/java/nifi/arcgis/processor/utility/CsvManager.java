/**
 * 
 */
package nifi.arcgis.processor.utility;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility Class for working with CSV content.
 * 
 * @author Fr&eacute;d&eacute;ric VIDAL (The CSV class is mainly inspired from MKyons)
 * @see https://www.mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
 */
public class CsvManager {

	/**
	 * The default SEPARATOR
	 */
	private static final char DEFAULT_SEPARATOR = ',';
	
	/**
	 * The default QUOTE
	 */
	private static final char DEFAULT_QUOTE = '"';

	
	/**
	 * Parse a CSV and return a list a String value representing each value of the line 
	 * with the default values for separator and quote.
	 * 
	 * @param cvsLine the line
	 * @return list of entries parsed from the line
	 */
	public static List<String> parseLine(String cvsLine) {
		return parseLine(cvsLine, DEFAULT_SEPARATOR, DEFAULT_QUOTE);
	}

	/**
	 * Parse a CSV and return a list a String value representing each value of the line
	 * with the default value for quote.
	 * 
	 * @param cvsLine the line
	 * @param separators the char used for separator such as <b><code>;</code></b> or <b><code>,</code></b>
	 * @return list of entries parsed from the line
	 */
	public static List<String> parseLine(String cvsLine, char separators) {
		return parseLine(cvsLine, separators, DEFAULT_QUOTE);
	}

	/**
	 * Parse a CSV and return a list a String value representing each value of the line
	 * @param cvsLine the line
	 * @param separators the char used for separator such as <b><code>;</code></b> or <b><code>,</code></b>
	 * @param customQuote the type of quote if necessary
	 * @return list of entries parsed from the line
	 */
	public static List<String> parseLine(String cvsLine, char separators, char customQuote) {

		List<String> result = new ArrayList<>();

		// if empty, return!
		if (cvsLine == null || cvsLine.isEmpty()) {
			return result;
		}

		if (customQuote == ' ') {
			customQuote = DEFAULT_QUOTE;
		}

		if (separators == ' ') {
			separators = DEFAULT_SEPARATOR;
		}

		StringBuffer curVal = new StringBuffer();
		boolean inQuotes = false;
		boolean startCollectChar = false;
		boolean doubleQuotesInColumn = false;

		char[] chars = cvsLine.toCharArray();

		for (char ch : chars) {

			if (inQuotes) {
				startCollectChar = true;
				if (ch == customQuote) {
					inQuotes = false;
					doubleQuotesInColumn = false;
				} else {

					// Fixed : allow "" in custom quote enclosed
					if (ch == '\"') {
						if (!doubleQuotesInColumn) {
							curVal.append(ch);
							doubleQuotesInColumn = true;
						}
					} else {
						curVal.append(ch);
					}

				}
			} else {
				if (ch == customQuote) {

					inQuotes = true;

					// Fixed : allow "" in empty quote enclosed
					if (chars[0] != '"' && customQuote == '\"') {
						curVal.append('"');
					}

					// double quotes in column will hit this!
					if (startCollectChar) {
						curVal.append('"');
					}

				} else if (ch == separators) {

					result.add(curVal.toString());

					curVal = new StringBuffer();
					startCollectChar = false;

				} else if (ch == '\r') {
					// ignore LF characters
					continue;
				} else if (ch == '\n') {
					// the end, break!
					break;
				} else {
					curVal.append(ch);
				}
			}

		}

		result.add(curVal.toString());

		return result;
	}

}
