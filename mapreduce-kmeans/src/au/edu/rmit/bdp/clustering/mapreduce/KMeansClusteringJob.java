package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.edu.rmit.bdp.clustering.model.DataPoint;
import au.edu.rmit.bdp.distance.DistanceMeasurer;
import au.edu.rmit.bdp.distance.EuclidianDistance;


public class KMeansClusteringJob {

	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
	private static Path dataset;
	private static Path outputPath;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        long startTime = System.nanoTime();
		
		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
		
		Path PointDataPath = new Path("clustering/data.seq");
		Path centroidDataPath = new Path("clustering/centroid.seq");
		conf.set("centroid.path", centroidDataPath.toString());
		Path outputDir = new Path("clustering/depth_1");

		Job job = Job.getInstance(conf);
		job.setJobName("KMeans Clustering");

		job.setMapperClass(KMeansMapper.class);
		job.setPartitionerClass(KMeansPartitioner.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		FileInputFormat.addInputPath(job, PointDataPath);
		FileSystem fs = FileSystem.get(conf);

		au.edu.rmit.bdp.clustering.mapreduce.Configuration.load(new Path(args[0]), fs);
		dataset = new Path(args[1]);
		outputPath = new Path(args[2]);
		
		Path opPath = new Path(args[2] + "_opf");

		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		if (fs.exists(centroidDataPath)) {
			fs.delete(centroidDataPath, true);
		}

		if (fs.exists(PointDataPath)) {
			fs.delete(PointDataPath, true);
		}
		
		generateCentroid(conf, centroidDataPath, fs);
		generateDataPoints(conf, PointDataPath, fs);

		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Centroid.class);
		job.setOutputValueClass(DataPoint.class);

		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", centroidDataPath.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setPartitionerClass(KMeansPartitioner.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			PointDataPath = new Path("clustering/depth_" + (iteration - 1) + "/");
			outputDir = new Path("clustering/depth_" + iteration);

			FileInputFormat.addInputPath(job, PointDataPath);
			if (fs.exists(outputDir))
				fs.delete(outputDir, true);

			FileOutputFormat.setOutputPath(job, outputDir);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Centroid.class);
			job.setOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);

			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path("clustering/depth_" + (iteration - 1) + "/");

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		if (fs.exists(opPath)) {
			fs.delete(opPath, true);
		}

		FSDataOutputStream outputFileStream = fs.create(outputPath);
		FSDataOutputStream opfFileStream = fs.create(opPath);

		HashMap<Centroid, DistancePairs> distanceMap = new HashMap<>();
		
		DistanceMeasurer measure = new EuclidianDistance();
		
		FileStatus[] stati = fs.listStatus(result);
		for (FileStatus status : stati) {
			if (!status.isDirectory()) {
				Path path = status.getPath();
				if (!path.getName().equals("_SUCCESS")) {
					LOG.info("FOUND " + path.toString());
					try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
							PrintWriter writer = new PrintWriter(outputFileStream, true)) {
						Centroid key = new Centroid();
						DataPoint v = new DataPoint();
						while (reader.next(key, v)) {
							// LOG.info(key + " / " + v);
							
							if (!distanceMap.containsKey(key)) {
								distanceMap.put(new Centroid(key), 
										new DistancePairs(Math.abs(measure.measureDistance(v.getVector(), key.getCenterVector())), 1));
							} else {
								DistancePairs dp = distanceMap.get(key);
								dp.increment(new DistancePairs(Math.abs(measure.measureDistance(v.getVector(), key.getCenterVector())), 1));
								distanceMap.put(new Centroid(key), dp);
							}
							
							String tempWriting = key.getCenterVector() + ", " + v.getVector();
							tempWriting = tempWriting.replaceAll("[\\[\\](){}]","");
							writer.println(tempWriting);
						}
					}
				}
			}
		}
		
        long endTime = System.nanoTime();
		long totalTime = (endTime - startTime) / 1000000;
		
		double totalDistanceSquare = 0.0;
		int totalAmount = 0;
		
		for (Map.Entry<Centroid, DistancePairs> entry : distanceMap.entrySet()) {
			totalDistanceSquare += entry.getValue().distance * entry.getValue().distance;
			totalAmount += entry.getValue().occurrence;
		}
		
		double functionValue = totalDistanceSquare / totalAmount;
		
		try (PrintWriter writer = new PrintWriter(opfFileStream, true)) {
			writer.println("Number of clusters: " + distanceMap.size());
			writer.println("Objective function value: " + functionValue);
			writer.println("Total time cost: " + totalTime + " ms");
			writer.println("Iterations: " + (iteration - 1));
		}
		
	}
	
	
	private static class DistancePairs {
		double distance;
		int occurrence;
		
		public DistancePairs(double distance, int occurrence) {
			this.distance = distance;
			this.occurrence = occurrence;
		}
			
		public void increment(DistancePairs dp) {
			this.distance += dp.distance;
			this.occurrence += dp.occurrence;
		}
	}
	

	@SuppressWarnings("deprecation")
	public static void generateDataPoints(Configuration conf, Path in, FileSystem fs) throws IOException {
		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Centroid.class,
				DataPoint.class)) {
			FSDataInputStream inputFileStream = fs.open(dataset);
			Scanner scanner = new Scanner(new InputStreamReader(inputFileStream));
			DataPoint initCent = new DataPoint(au.edu.rmit.bdp.clustering.mapreduce.Configuration.CITY_LONGITUDE,
					au.edu.rmit.bdp.clustering.mapreduce.Configuration.CITY_LATITUDE);

			scanner.nextLine();

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] temp = line.split(",");

				try {
					double lo = Double.parseDouble(temp[0]);
					double la = Double.parseDouble(temp[1]);
					if (la == 0 || lo == 0) {
						throw new NumberFormatException();
					}
					dataWriter.append(new Centroid(initCent), new DataPoint(lo, la));
				} catch (NumberFormatException ignored) {}
			}

			scanner.close();
		}
	}

	
	@SuppressWarnings("deprecation")
	public static void generateCentroid(Configuration conf, Path center, FileSystem fs) throws IOException {
		try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
				IntWritable.class)) {
			final IntWritable value = new IntWritable(0);
			for (int i = 0; i < au.edu.rmit.bdp.clustering.mapreduce.Configuration.K; i++) {
				Random random = new Random();
				double lo = au.edu.rmit.bdp.clustering.mapreduce.Configuration.CITY_LONGITUDE -
						au.edu.rmit.bdp.clustering.mapreduce.Configuration.RANGE +
						au.edu.rmit.bdp.clustering.mapreduce.Configuration.RANGE * 2 * random.nextDouble();
				double la = au.edu.rmit.bdp.clustering.mapreduce.Configuration.CITY_LATITUDE -
						au.edu.rmit.bdp.clustering.mapreduce.Configuration.RANGE +
						au.edu.rmit.bdp.clustering.mapreduce.Configuration.RANGE * 2 * random.nextDouble();
				centerWriter.append(new Centroid(new DataPoint(lo, la)), value);
				System.out.println("longitude " + lo + " latitude " + la);
			}
		}
	}

}
