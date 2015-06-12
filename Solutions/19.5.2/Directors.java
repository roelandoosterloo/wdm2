import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Directors {
	public static class DirectorMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text director = new Text();
		private Text movie = new Text();
		private boolean inDirectorTag = false;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().contains("<title>")) {
				movie.set(value.toString().replaceAll("<[^>]+>", "").trim());
			}
			
			if(value.toString().contains("<year>")) {
				movie.set(movie.toString() + "\t" + value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+",""));
			}			
		
			if(value.toString().contains("<director>")) {
				director.set("");
				inDirectorTag = true;
			}

			if(value.toString().contains("<last_name>") && inDirectorTag) {
				director.set(director.toString() + value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+",""));
			}			
			
			if(value.toString().contains("<first_name>") && inDirectorTag) {
				director.set(value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+","") + " " + director.toString());
			}
			
			if(value.toString().contains("</director>")) {
				context.write(director, movie);
			}			
		}
	}
	
	public static class DirectorReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text val:values) {
				context.write(key, val);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if(args.length != 2) {
			System.err.println("Usage: MoviesJob <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Counting movies");
		
		job.setMapperClass(DirectorMapper.class);
		job.setReducerClass(DirectorReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
