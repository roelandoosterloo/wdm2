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

public class Actors {
	public static class ActorMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text title = new Text();
		private Text actor = new Text();
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().contains("<title>")) {
				title.set(value.toString().replaceAll("<[^>]+>", "").trim());
			}
		
			if(value.toString().contains("<actor>")) {
				actor.set("");
			}

			if(value.toString().contains("<first_name>")) {
				actor.set(actor.toString() + "" + value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+",""));
			}
			
			if(value.toString().contains("<last_name>")) {
				actor.set(actor.toString() + " " + value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+",""));
			}
			
			if(value.toString().contains("<birth_date>")) {
				actor.set(actor.toString() + "\t" + value.toString().replaceAll("<[^>]+>", "").replaceAll("\\s+",""));
			}			
			
			if(value.toString().contains("<role>")) {
				actor.set(actor.toString() + "\t" + value.toString().replaceAll("<[^>]+>", "").trim());
			}
			
			if(value.toString().contains("</actor>")) {
				context.write(title, actor);
				actor = new Text();
			}			
		}
	}
	
	public static class ActorReducer extends Reducer<Text, Text, Text, Text> {
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
		
		job.setMapperClass(ActorMapper.class);
		job.setReducerClass(ActorReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
