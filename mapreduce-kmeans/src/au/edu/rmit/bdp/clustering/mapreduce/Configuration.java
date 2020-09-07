package au.edu.rmit.bdp.clustering.mapreduce;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Configuration {

    public static String CITY_NAME;
	public static double CITY_LONGITUDE;
	public static double CITY_LATITUDE;
	public static double RANGE;
	public static int K;

	public static void load(Path filePath, FileSystem fs) throws IOException {

        FSDataInputStream inputFileStream = fs.open(filePath);
        Scanner scanner = new Scanner(new InputStreamReader(inputFileStream));

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] temp = line.split(":");

            if (temp[0].equals("CITY_NAME")) {
                CITY_NAME = temp[1];
            } else if (temp[0].equals("CITY_LONGITUDE")) {
                CITY_LONGITUDE = Double.parseDouble(temp[1]);
            } else if (temp[0].equals("CITY_LATITUDE")) {
                CITY_LATITUDE = Double.parseDouble(temp[1]);
            } else if (temp[0].equals("RANGE")) {
                RANGE = Double.parseDouble(temp[1]);
            } else if (temp[0].equals("K")) {
                K = Integer.parseInt(temp[1]);
            } else {
                continue;
            }
        }

        scanner.close();

	}

}
