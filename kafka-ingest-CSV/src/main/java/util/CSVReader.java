package util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVReader {
	/* Read the given CSV file and return a list of all lines in the file
	 * after skipping header.
	 */
	public static List<String> getLines(String filePath) {
		List<String> lines = new ArrayList<>();
		BufferedReader br = null;
		String line = "";
	
		try {
			br = new BufferedReader(new FileReader(filePath));
            br.readLine(); //drop Header
            while ((line = br.readLine()) != null) {
            	lines.add(line);
            }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
		}
		
		return lines;
	}
}
