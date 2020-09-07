package au.edu.rmit.bdp.clustering.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import au.edu.rmit.bdp.clustering.model.DataPoint;

//Partitioner class
public class KMeansPartitioner extends Partitioner <Text, Text> {
	
	private static final Logger LOG = Logger.getLogger(KMeansPartitioner.class);
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		// Set log-level to debugging
		LOG.setLevel(Level.DEBUG);
		
		String[] str = value.toString().split(",");
		double lo = Double.parseDouble(str[0]);
		double la = Double.parseDouble(str[1]);
		DataPoint point = new DataPoint(lo, la);
		
		if (numReduceTasks == 0) {
			LOG.debug("No partitioning - only ONE reducer");
			return 0;
		}
		
		if (point.hashCode() % 3 == 0) {
			LOG.debug(point + " is directed to Partition: 0");
			return 0;
		}
		else if (point.hashCode() % 3 == 1) {
			LOG.debug(point + " is directed to Partition: " + Integer.toString(1 % numReduceTasks));
			return 1 % numReduceTasks;
		}
		else {
			LOG.debug(point + " is directed to Partition: " + Integer.toString(2 % numReduceTasks));
  	 	   	return 2 % numReduceTasks;
		}
	}
}
