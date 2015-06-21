
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
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, RelationWritable> {
   
    private JoinWritable hashKey = new JoinWritable();
    private JoinWritable hashKeyRev = new JoinWritable();
    private RelationWritable relation = new RelationWritable();
    private RelationWritable relationRev = new RelationWritable();
    private final static int m = 3;
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        
            String line = value.toString();
            
            String[] words =  line.split(" ");
            
            if(words.length >= 2){
                int a = Integer.parseInt(words[0]);
                int b = Integer.parseInt(words[1]);
                
                for(int i = 0 ; i <= m; i++){
                    hashKey.set(a,b,i,m);
                    relation.set(a,b,"A");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                    
                    hashKey.set(a,i,b,m);
                    relation.set(a,b,"B");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                    
                    hashKey.set(i,a,b,m);
                    relation.set(a,b,"C");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                }
 
            }
            
    }
 } 
 
 public static class Reduce extends Reducer<IntWritable, RelationWritable, IntWritable, Text> {

    public List<RelationWritable> a_relation = new ArrayList<RelationWritable>();
    public List<RelationWritable> b_relation = new ArrayList<RelationWritable>();
    public List<RelationWritable> c_relation = new ArrayList<RelationWritable>();
     
    private IntWritable result = new IntWritable();
    
    public void reduce(IntWritable key, Iterable<RelationWritable> values, Context context) 
      throws IOException, InterruptedException {
                
        int count = 0;
        
        List<Edge> cacheA = new ArrayList<Edge>();
        List<Edge> cacheB = new ArrayList<Edge>();
        List<Edge> cacheC = new ArrayList<Edge>();
        
                
        for(RelationWritable value : values) {
            if(value.getRelation().equals("A")){ 
                Edge e = new Edge(value.getA(), value.getB());
                if(!cacheA.contains(e)){
                    cacheA.add(e);
                }
            } else if (value.getRelation().equals("B")) {
            	Edge e = new Edge(value.getA(), value.getB());
                if(!cacheB.contains(e)){
                    cacheB.add(e);
                }                
            } else {
            	Edge e = new Edge(value.getA(), value.getB());
                if(!cacheC.contains(e)){
                    cacheC.add(e);
                }
            }
        }
        context.write(key, new Text(printCache(cacheA)));
        context.write(key, new Text(printCache(cacheB)));
        context.write(key, new Text(printCache(cacheC)));
                
        for(int i = 0; i < cacheA.size(); i++) {
            Edge e1 = cacheA.get(i);
            Triangle t = new Triangle();
            t.setA(e1);
            for(int j = 0; j < cacheB.size(); j++) {
                Edge e2 = cacheB.get(j);
                if(!t.canSetB(e2)){
                    continue;
                } else {
                	t.setB(e2);
                    for(int k = 0; k < cacheC.size(); k++) {
                        Edge e3 = cacheC.get(k);
                        if(t.canSetC(e3)) {
                            count++;
                            break;
                        }
                    }
                }
            }
        }
        result.set(count);
        
//        context.write(new Text("Triangles"), result);
        
    }
    
    private String printCache(List<Edge> cache) {
    	String result = "";
    	for(int i = 0; i < cache.size(); i++) {
    		result = result + cache.get(i).toString() + ",";
    	}
    	return result;
    }
 }
       
 public static class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		 int sum = 0;
		 IntWritable result = new IntWritable();
		 for(IntWritable value : values) {
			 sum += value.get();
		 }
		 result.set(sum);
		 context.write(key, result);
	 }
 }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = Job.getInstance(conf);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(RelationWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
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