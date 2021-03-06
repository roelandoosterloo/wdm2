
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
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
    private RelationWritable relrev = new RelationWritable();
    private final static int m = 3;
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        
            String line = value.toString();
            
            String[] words =  line.split(" ");
            
            if(words.length >= 2){
                int a = Integer.parseInt(words[0]);
                int b = Integer.parseInt(words[1]);
                
                for(int i = 0 ; i < m; i++){
                    hashKey.set(a,b,i,m);
                    hashKeyRev.set(b,a,i,m);
                    relation.set(a,b,"A");
                    relrev.set(b,a,"A");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                    context.write(new IntWritable(hashKeyRev.hashCode()), relrev);
                    
                    hashKey.set(i,a,b,m);
                    hashKeyRev.set(i,b,a,m);
                    relation.set(a,b,"B");
                    relrev.set(b,a,"B");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                    context.write(new IntWritable(hashKeyRev.hashCode()), relrev);
                    
                    hashKey.set(a,i,b,m);
                    hashKeyRev.set(b,i,a,m);
                    relation.set(a,b,"C");
                    relrev.set(b, a, "C");
                    context.write(new IntWritable(hashKey.hashCode()), relation);
                    context.write(new IntWritable(hashKeyRev.hashCode()), relrev);
                }
 
            }
            
    }
 } 
 
 public static class Reduce extends Reducer<IntWritable, RelationWritable, IntWritable, IntWritable> {

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
               if(!cacheA.contains(value)) {
                    cacheA.add(new Edge(value.getA(), value.getB()));
                }
            } else if (value.getRelation().equals("B")) {
            	if(!cacheB.contains(value)) {
                    cacheB.add(new Edge(value.getA(), value.getB()));
                }
                
            } else {
            	if(!cacheC.contains(value)) {
                    cacheC.add(new Edge(value.getA(), value.getB()));
                }
            }
        }

        
//        context.write(key, new Text(cacheWriter(cacheA)));
//        context.write(key, new Text(cacheWriter(cacheB)));
//        context.write(key, new Text(cacheWriter(cacheC)));
        
//        for(Edge temp: cacheA){
//            context.write(key,new Text(temp.getA() + " " + temp.getB()));
//        }
//        
//        for(Edge temp: cacheB){
//            context.write(key,new Text(temp.getA() + " " + temp.getB()));
//        }
//        
//        for(Edge temp: cacheC){
//            context.write(key,new Text(temp.getA() + " " + temp.getB()));
//        }
                
        for(int i = 0; i < cacheA.size(); i++) {
            Edge e1 = cacheA.get(i);
            for(int j = 0; j < cacheB.size(); j++) {
                Edge e2 = cacheB.get(j);
                if(e1.getB() != e2.getA()){
                    continue;
                }else{
                    for(int k = 0; k < cacheC.size(); k++) {
                        Edge e3 = cacheC.get(k);
                        if(e2.getB() == e3.getB() && e3.getA() == e1.getA()) {
                            count++;
                            break;
                        }
                    }
                }
            }
        }
        result.set(count);
        
        context.write(key, result);
        
    }
    
    public String cacheWriter(List<Edge> cache) {
    	String ret = "";
    	for(Edge e : cache) {
    		ret = ret + " , " + e.toString();
    	}
    	return ret;
    }
 }
 
 public static class IdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 IntWritable res = new IntWritable();
		 String temp = value.toString();
		 int x = Character.getNumericValue(temp.charAt(temp.length()-1));
		 res.set(x);
		 context.write(new Text("Totaal"), res);
	 }
 }
 
 public static class AggregateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		 int total = 0;
		 for(IntWritable value : values) {
			 total += value.get();
			 //context.write(new Text("Total"), new IntWritable(total));
		 }
		 
		 context.write(new Text("Total"), new IntWritable(total/6));
	 }
 }
        
 public static void main(String[] args) throws Exception {
	  
    Configuration conf = new Configuration();
     
    FileSystem fs = FileSystem.get(conf);
	fs.delete(new Path("temp1"), true);
    
    Job job = Job.getInstance(conf);
    job.setJobName("Counting");

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
    FileOutputFormat.setOutputPath(job, new Path("temp1"));
    
    
    Job agg = Job.getInstance(conf);
    agg.setJobName("Aggregate");
    
    agg.setMapOutputKeyClass(Text.class);
    agg.setMapOutputValueClass(IntWritable.class);
    
    agg.setOutputKeyClass(Text.class);
    agg.setOutputValueClass(IntWritable.class);
    
    agg.setMapperClass(IdMapper.class);
    agg.setReducerClass(AggregateReducer.class);
    
    agg.setInputFormatClass(TextInputFormat.class);
    agg.setOutputFormatClass(TextOutputFormat.class);
    
    
    FileInputFormat.setInputPaths(agg, new Path("temp1"));
    FileOutputFormat.setOutputPath(agg, new Path(args[1]));
        
    job.waitForCompletion(true);
    agg.waitForCompletion(true);
 }
        
}