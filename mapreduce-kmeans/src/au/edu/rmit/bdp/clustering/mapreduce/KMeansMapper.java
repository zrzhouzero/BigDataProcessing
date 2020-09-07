package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.edu.rmit.bdp.distance.DistanceMeasurer;
import au.edu.rmit.bdp.distance.EuclidianDistance;
import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import au.edu.rmit.bdp.clustering.model.DataPoint;


public class KMeansMapper extends Mapper<Centroid, DataPoint, Centroid, DataPoint> {

	private final List<Centroid> centers = new ArrayList<>();
	private DistanceMeasurer distanceMeasurer;
	
	private HashMap<Centroid, HashMap<DataPoint, Integer>> map;

    @SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Centroid centroid = new Centroid(key);
				centroid.setClusterIndex(index++);
				centers.add(centroid);
			}
		}

		map = new HashMap<>();
		distanceMeasurer = new EuclidianDistance();
	}

	
	@Override
	protected void map(Centroid centroid, DataPoint dataPoint, Context context) throws IOException,
			InterruptedException {

		Centroid nearest = null;
		double nearestDistance = Double.MAX_VALUE;

		for (Centroid c : centers) {
			double distance = distanceMeasurer.measureDistance(c.getCenterVector(), dataPoint.getVector());
			if (nearest == null) {
				nearest = c;
				nearestDistance = distance;
			} else {
				if (nearestDistance > distance) {
					nearest = c;
					nearestDistance = distance;
				}
			}
		}
				
		HashMap<DataPoint, Integer> dataPointOccurrence;
		
		if (map.containsKey(nearest)) {
			dataPointOccurrence = map.get(nearest);
			if (dataPointOccurrence.containsKey(dataPoint)) {
				Integer i = dataPointOccurrence.get(dataPoint);
				i++;
				dataPointOccurrence.put(new DataPoint(dataPoint), i);
			} else {
				dataPointOccurrence.put(new DataPoint(dataPoint), 1);
			}
		} else {
			dataPointOccurrence = new HashMap<>();
			dataPointOccurrence.put(new DataPoint(dataPoint), 1);
		}
		
		map.put(nearest, dataPointOccurrence);
		
		// System.out.println(nearest + " " + dataPoint);
		// context.write(nearest, dataPoint);
		
	}

	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for	(Map.Entry<Centroid, HashMap<DataPoint, Integer>> entry : map.entrySet()) {
						
			Centroid c = entry.getKey();
			HashMap<DataPoint, Integer> d = entry.getValue();
			
			for (Map.Entry<DataPoint, Integer> data : d.entrySet()) {
				
				int i = data.getValue();
				
				for (int j = 0; j < i; j++) {
					context.write(c, data.getKey());
				}
			}
			
		}
		
	}
	
}
