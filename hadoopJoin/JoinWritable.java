
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class JoinWritable implements WritableComparable {    
    private IntWritable a;
    private IntWritable b;
    private IntWritable c;
    
    @Override
    public void write(DataOutput out) throws IOException {
      a.write(out);
      b.write(out);
      c.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      a.readFields(in);
      b.readFields(in);
      c.readFields(in);
    }
    
    public void set(int a, int b, int c, int m) {
    	this.a =  new IntWritable(a % m);
    	this.b = new IntWritable(b % m);
    	this.c = new IntWritable(c % m);
    }

    public int getA() {
    	return this.a.get();
    }
    
    public int getB() {
    	return this.b.get();
    }
    
    public int getC() {
    	return this.c.get();
    }
    
    public static JoinWritable read(DataInput in) throws IOException {
      JoinWritable w = new JoinWritable();
      w.readFields(in);
      return w;
    }
    
    public JoinWritable(){
    	this.a = new IntWritable();
    	this.b = new IntWritable();
    	this.c = new IntWritable();
    }
	@Override
	public int compareTo(Object o) {
		
		if(  o instanceof JoinWritable){
			JoinWritable temp = (JoinWritable) o;
			if(this.a == temp.a && this.b == temp.b && this.c == temp.c){
				return 1;
			}else{
				return 0;
			}
		}else{
			return 0;
		}
		
		
	}
	
	@Override
	public String toString(){
		return "key is" + a.get() + b.get() + c.get();
	}
    
    
    

}