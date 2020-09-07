package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.rmit.bdp.clustering.model.DataPoint;
import de.jungblut.math.DoubleVector;


public class KMeansReducer extends Reducer<Centroid, DataPoint, Centroid, DataPoint> {

	
	public static enum Counter {
		CONVERGED
	}
	

	private final List<Centroid> centers = new ArrayList<>();


	@Override
	protected void reduce(Centroid centroid, Iterable<DataPoint> dataPoints, Context context) throws IOException,
			InterruptedException {

		List<DataPoint> vectorList = new ArrayList<>();

		// compute the new centroid
		DoubleVector newCenter = null;
		for (DataPoint value : dataPoints) {
			vectorList.add(new DataPoint(value));
			if (newCenter == null)
				newCenter = value.getVector().deepCopy();
			else
				newCenter = newCenter.add(value.getVector());
		}
		newCenter = newCenter.divide(vectorList.size());
		Centroid newCentroid = new Centroid(newCenter);
		centers.add(newCentroid);

		// write new key-value pairs to disk, which will be fed into next round mapReduce job.
		for (DataPoint vector : vectorList) {
			context.write(newCentroid, vector);
		}

		// check if all centroids are converged.
		// If all of them are converged, the counter would be zero.
		// If one or more of them are not, the counter would be greater than zero.
		if (newCentroid.update(centroid))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	
	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
				Centroid.class, IntWritable.class)) {

			final IntWritable value = new IntWritable(0);
			for (Centroid c : centers) {
				out.append(c, value);
			}
		}
	}
}
