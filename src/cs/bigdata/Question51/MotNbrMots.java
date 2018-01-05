package cs.bigdata.Question51;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class  MotNbrMots implements WritableComparable <MotNbrMots>{

	private Text mot;
	private IntWritable nbrMots;
	
	public MotNbrMots() {
		this.mot=new Text();
		this.nbrMots=new IntWritable();

	}

	public MotNbrMots(Text mot,int nbrMots) {
		this.mot=mot;
		this.nbrMots=new IntWritable(nbrMots);

	}



	@Override
	public void readFields(DataInput input) throws IOException {
		mot.readFields(input);
		nbrMots.readFields(input);
		
	}



	@Override
	public void write(DataOutput output) throws IOException {
		mot.write(output);
		nbrMots.write(output);
	}



	@Override
	public int compareTo(MotNbrMots o) {
		// TODO Auto-generated method stub
		return mot.compareTo(o.mot);
		
	}
	
	public void set(String mot,int nbrMots) {
		// TODO Auto-generated method stub
		this.mot=new Text(mot);
		this.nbrMots=new IntWritable(nbrMots);
	}

	public void set(Text mot, int nbrMot) {
		// TODO Auto-generated method stub
		this.mot=mot;
		this.nbrMots=new IntWritable(nbrMot);
	}
	
	public IntWritable getNbrMots() {
		// TODO Auto-generated method stub
		return this.nbrMots;
	}
	
	

	public Text getMot() {
		// TODO Auto-generated method stub
		Text t=new Text();
		t.set(mot);
		return t;
	}

	
	
	


	
}
	