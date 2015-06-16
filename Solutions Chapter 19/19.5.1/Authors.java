import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Authors {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    
	private final static IntWritable one = new IntWritable(1);
    private Text author = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Scanner line = new Scanner(value.toString());
        line.useDelimiter("\t");
        author.set(line.next());
        context.write(author, one);
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value:  values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
 }
        
 public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(Authors.Map.class);
	    job.setCombinerClass(Authors.Reduce.class); // answer Exercise 19.5.1
	    job.setReducerClass(Authors.Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}