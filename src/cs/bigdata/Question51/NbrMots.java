package cs.bigdata.Question51;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class  NbrMots implements WritableComparable <NbrMots>{

	private IntWritable motsParDoc;
	private IntWritable nbrMots;
	
	//constructeurs
	public NbrMots() {
		this.motsParDoc=new IntWritable();
		this.nbrMots=new IntWritable();

	}

	public NbrMots(IntWritable nbrMots, IntWritable motsParDoc) {
		this.motsParDoc=motsParDoc;
		this.nbrMots=nbrMots;

	}


	//mehtodes necessaires au MapReduce
	@Override
	public void readFields(DataInput input) throws IOException {
		(motsParDoc).readFields(input);
		(nbrMots).readFields(input);
		
	}



	@Override
	public void write(DataOutput output) throws IOException {
		motsParDoc.write(output);
		nbrMots.write(output);
	}



	@Override
	public int compareTo(NbrMots o) {
		// TODO Auto-generated method stub
		if(nbrMots.compareTo(o.nbrMots)==0) {
			return motsParDoc.compareTo(o.motsParDoc);
		}else {
			return nbrMots.compareTo(o.nbrMots);
		}
	}
	
	
	//set et get
	
	public void set(IntWritable nbrMots,IntWritable motsParDoc) {
		// TODO Auto-generated method stub
		this.nbrMots=nbrMots;
		this.motsParDoc=motsParDoc;
	}

	public void set(int nbrMots, int motsParDoc) {
		// TODO Auto-generated method stub
		this.nbrMots=new IntWritable(nbrMots);
		this.motsParDoc=new IntWritable(motsParDoc);
	}
	
	public void set(IntWritable nbrMots, int motsParDoc) {
		// TODO Auto-generated method stub
		this.nbrMots=nbrMots;
		this.motsParDoc=new IntWritable(motsParDoc);
	}

	public IntWritable getNbrMots() {
		// TODO Auto-generated method stub
		return this.nbrMots;
	}
	
	public IntWritable getMotsParDoc() {
		// TODO Auto-generated method stub
		return this.motsParDoc;
	}
	
	//transforme le couple en un texte
	public Text toText() {
		return new Text(nbrMots.toString()+";"+motsParDoc.toString());
	}






	
}
	