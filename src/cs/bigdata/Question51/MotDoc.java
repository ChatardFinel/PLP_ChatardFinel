package cs.bigdata.Question51;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class  MotDoc implements WritableComparable <MotDoc>{

	private Text mot;
	private Text docID;
	
	//constructeurs
	public MotDoc() {
		this.mot=new Text();
		this.docID=new Text();

	}

	public MotDoc(Text mot, Text docID) {
		this.mot=mot;
		this.docID=docID;

	}


	//methodes spécifiques au MapReduce
	@Override
	public void readFields(DataInput input) throws IOException {
		(mot).readFields(input);
		(docID).readFields(input);
		
	}



	@Override
	public void write(DataOutput output) throws IOException {
		mot.write(output);
		docID.write(output);
	}


	public int compareTo(MotDoc o) {
		// TODO Auto-generated method stub
		if(mot.compareTo(o.mot)==0) {
			return docID.compareTo(o.docID);
		}else {
			return mot.compareTo(o.mot);
		}
		
	}
	
	//set et get
	
	public void set(String mot,Text docID) {
		// TODO Auto-generated method stub
		this.mot=new Text(mot);
		this.docID=docID;
	}
	
	public void set(Text mot, Text docID) {
		// TODO Auto-generated method stub
		this.mot=mot;
		this.docID=docID;
	}
	
	
	public void set(Text mot, String docID) {
		// TODO Auto-generated method stub
		this.mot=mot;
		this.docID=new Text(docID);
	}
	
	public Text getMot() {
		return this.mot;
	}
	
	public Text getDocID() {
		return this.docID;
	}

	
	public Text toText() {
		String s="";
		try {
		s= mot.toString()+";";
		}
		catch (NullPointerException e) {
			s="missingWord"+";";
		}try {
			s=s+docID.toString();
		}catch (NullPointerException e) {
			s=s+"missingDocID";
		}
		return new Text(s);
		
	}
	
}
	