package cs.bigdata.Question52;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Couple implements WritableComparable <Couple> {
	
	private Text page;
	private DoubleWritable pagerank;
	
	//constructeur
	public Couple() {
		this.page=new Text();
		this.pagerank=new DoubleWritable();
		
	}

	
	//specifique au MapReduce
	@Override
	public void readFields(DataInput input) throws IOException {
		page.readFields(input);
		pagerank.readFields(input);
		
	}
	
	
	@Override
	public void write(DataOutput output) throws IOException {
		page.write(output);
		pagerank.write(output);
		
	}
	
	@Override
	public int compareTo(Couple o) {
		return pagerank.compareTo(o.pagerank);
		
	}
	
	//set et get
	public void set(String page,double pagerank) {
		this.page=new Text(page);
		this.pagerank=new DoubleWritable(pagerank);
	}
	
	
	public void set(String s) {
		this.page=new Text(s.split(",")[0]);
		this.pagerank=new DoubleWritable(Double.parseDouble(s.split(",")[1]));
		
	}
	
	public DoubleWritable getPageRank() {
		return this.pagerank;
	}
	
	public Text getPage() {
		Text p=new Text();
		p.set(page);
		return p;
	}
	
	//pour transformer un couple en texte
	public Text toText() {
		Text p=new Text();
		p.set(page+","+pagerank);
		return p;
	}
	
}

