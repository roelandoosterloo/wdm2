
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
        		
        		a = 1;
        		b = 1;
        		
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
 
 public static class Reduce extends Reducer<JoinWritable, RelationWritable, JoinWritable, IntWritable> {

 	private IntWritable result = new IntWritable();
	 
    public void reduce(JoinWritable key, Iterable<RelationWritable> values, Context context) 
      throws IOException, InterruptedException {
    	int count = 0;
    	
    	List<Edge> cacheA = new ArrayList<Edge>();
    	List<Edge> cacheB = new ArrayList<Edge>();
    	List<Edge> cacheC = new ArrayList<Edge>();
    	
    	for(RelationWritable value : values) {
    		if(value.getRelation().equals("A")){ 
    			cacheA.add(new Edge(value.getA(), value.getB()));
    		} else if (value.getRelation().equals("B")) {
    			cacheB.add(new Edge(value.getA(), value.getB()));
    		} else {
    			cacheC.add(new Edge(value.getA(), value.getB()));
    		}
    	}
    	
    	for(int i = 0; i < cacheA.size(); i++) {
    		Edge e1 = cacheA.get(i);
    		for(int j = 0; j < cacheB.size(); j++) {
    			Edge e2 = cacheB.get(j);
    			if(e1.getB() != e2.getA()){
    				continue;
    			}
    			for(int k = 0; k < cacheC.size(); k++) {
    				Edge e3 = cacheC.get(k);
    				if(e2.getB() == e3.getA() && e3.getB() == e1.getA()) {
    					count++;
    					break;
    				}
    			}
    		}
    	}
    	result.set(count);
    	
    	context.write(key, result);
    	
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