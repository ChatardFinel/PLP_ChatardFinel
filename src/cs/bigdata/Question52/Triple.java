package cs.bigdata.Question52;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Triple implements WritableComparable <Triple> {
	
	private Text page;
	private DoubleWritable pagerank;
	private IntWritable nbrpage;

	//constructeur
	public Triple() {
		this.page=new Text();
		this.pagerank=new DoubleWritable();
		this.nbrpage=new IntWritable();
		
	}

	//specifique au MapReduce
	
	
	@Override
	public void readFields(DataInput input) throws IOException {
		page.readFields(input);
		pagerank.readFields(input);
		nbrpage.readFields(input);
		
	}
	
	
	@Override
	public void write(DataOutput output) throws IOException {
		page.write(output);
		pagerank.write(output);
		nbrpage.write(output);
	}
	
	
	public int compareTo(Triple o) {
		return page.compareTo(o.page);
		
	}
	
	//set et get
	
	public void set(Couple couple, int nbrpage) {
		this.page=couple.getPage();
		this.pagerank=couple.getPageRank();
		this.nbrpage=new IntWritable(nbrpage);
	}
	
	
	public Text getPage() {
		Text p=new Text();
		p.set(page);
		return p;
	}
	
	
	public Text getPageRankTxt() {
		Text p=new Text();
		p.set(pagerank.toString());
		return p;
	}
	
	
	public IntWritable getNbrPage() {
		return this.nbrpage;
	}
	
	
}

