
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
        
public class Join {
        
 public static class Map extends Mapper<LongWritable, Text, JoinWritable, RelationWritable> {
   
    private JoinWritable hashKey = new JoinWritable();
    private JoinWritable hashKeyRev = new JoinWritable();
    private RelationWritable relation = new RelationWritable();
    private final static int m = 3;
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	
    		String line = value.toString();
    		
    		String[] words =  line.split(" ");
    		
    		if(words.length >= 2){
    			int a = Integer.parseInt(words[0]);
        		int b = Integer.parseInt(words[1]);
        		
        	
        		
        		
        		for(int i = 0 ; i <= m; i++){
        			hashKey.set(a,b,i,m);
            		hashKeyRev.set(b,a,i,m);
            		relation.set(a,b,"A");
            		context.write(hashKey, relation);
            		context.write(hashKeyRev, relation);
            		
        			hashKey.set(a,i,b,m);
            		hashKeyRev.set(b,i,a,m);
            		relation.set(a,b,"B");
            		context.write(hashKey, relation);
            		context.write(hashKeyRev, relation);
            		
        			hashKey.set(i,a,b,m);
            		hashKeyRev.set(i,b,a,m);
            		relation.set(a,b,"C");
            		context.write(hashKey, relation);
            		context.write(hashKeyRev, relation);
        		}
 
    		}else{
    			hashKey.set(5,5,5,5);
        		relation.set(10,10,"B");
       
    			context.write(hashKey, relation);
    		}
    		
    }
 } 
 
 public static class Reduce extends Reducer<JoinWritable, RelationWritable, Text, Text> {

	public List<RelationWritable> a_relation = new ArrayList<RelationWritable>();
	public List<RelationWritable> b_relation = new ArrayList<RelationWritable>();
	public List<RelationWritable> c_relation = new ArrayList<RelationWritable>();
	 
    public void reduce(JoinWritable key, Iterable<RelationWritable> values, Context context) 
      throws IOException, InterruptedException {
    	for(RelationWritable val : values) {
    		
    		if(val.getRelation().equals("A")){
    			a_relation.add(val);
    		}else if(val.getRelation().equals("B")){
    			b_relation.add(val);
    		}else{
    			c_relation.add(val);
    		}
    	
    	
    	
    	}
    	
    	
    		context.write(new Text(key.toString()), new Text(a_relation.size()+""));
    		context.write(new Text(key.toString()), new Text(b_relation.size()+""));
    		context.write(new Text(key.toString()), new Text(c_relation.size()+""));
    	
 
    	/*for(RelationWritable val : a_relation){
    		context.write(new Text("OMG ROELAND IS EEN KONING"), new Text(val.getA() + " " +  val.getB() + " " + val.getRelation()));
    	}
    
    	for(RelationWritable val : b_relation){
    		context.write(new Text("OMG ROELAND IS EEN KONING"), new Text(val.getA() + " " +  val.getB() + " " + val.getRelation()));
    	}
    
    	for(RelationWritable val : c_relation){
    		context.write(new Text("OMG ROELAND IS EEN KONING"), new Text(val.getA() + " " +  val.getB() + " " + val.getRelation()));
    	}*/
    	
    
        
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "inverted");
    
        job.setMapOutputKeyClass(JoinWritable.class);
        job.setMapOutputValueClass(RelationWritable.class);
        
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    //job.setJarByClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}