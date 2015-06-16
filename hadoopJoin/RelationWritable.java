
import java.io.DataInput;
import java.io.DataOutput;
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

public class RelationWritable implements Writable {    
    private IntWritable a;
    private IntWritable b;
    private Text relation;
    
    public void write(DataOutput out) throws IOException {
      a.write(out);
      b.write(out);
      relation.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {

      a.readFields(in);
      b.readFields(in);
      relation.readFields(in);
      
    }
    
    public void set(int a, int b, String relation) {
    	this.a = new IntWritable(a);
    	this.b = new IntWritable(b);
    	this.relation = new Text(relation);
    }

    public int getA() {
    	return this.a.get();
    }
    
    public int getB() {
    	return this.b.get();
    }
    
    public String getRelation() {
    	return this.relation.toString();
    }
    
    public static RelationWritable read(DataInput in) throws IOException {
      RelationWritable w = new RelationWritable();
      w.readFields(in);
      return w;
    }
    
    public RelationWritable(){
    	this.a = new IntWritable();
    	this.b = new IntWritable();
    	this.relation = new Text();
    }
    
    

}