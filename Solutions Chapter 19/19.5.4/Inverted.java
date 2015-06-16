import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Inverted {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    
	private final static IntWritable doc1 = new IntWritable(1);
	private final static IntWritable doc2 = new IntWritable(2);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	 String temp = context.getInputSplit().toString();
        
         if(temp.contains("input1.txt")){
        	 String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        while (tokenizer.hasMoreTokens()) {
		            word.set(tokenizer.nextToken());
		            context.write(word,doc1);
		      }
         }else{
        	 String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        while (tokenizer.hasMoreTokens()) {
		            word.set(tokenizer.nextToken());
		            context.write(word, doc2);
		     }
         }
				
    }
 } 
 

        
 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
    	int count = 0;
    	HashMap<Integer, Boolean> docs = new HashMap<Integer, Boolean>();
    	
    	for(IntWritable val:values) {
    		count += 1;
    		docs.put(val.get(), true);
		}
    	
    	String message = "Frequency: " + count + " Contains in documents: ";
    	
    	for(int i = 0; i < 5; i++){
    		
    		if(docs.containsKey(i)){
    			if(docs.get(i)){
        			message = message + i + " ";
        		}
    		}
    		
    	}
    	
    
    	
    	context.write(key, new Text(message));
        
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "inverted");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setJarByClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
    job.waitForCompletion(true);
 }
        
}