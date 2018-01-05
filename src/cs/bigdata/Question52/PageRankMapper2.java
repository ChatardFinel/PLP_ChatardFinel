package cs.bigdata.Question52;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PageRankMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static Couple coupleTo = new Couple();
	private final static Couple coupleFrom = new Couple();
	private final static Triple tripleFrom = new Triple();
	
	

	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
	{
		// To complete according to the processing
		// Processing : keyI = ..., valI = ...
		
		//initialisation des variables
		Text txt = new Text();
		String str="";
		String coupleFromStr = values.toString().split("\t")[0];
		int nbrpage = Integer.parseInt(values.toString().split("\t")[1].split(";")[0]);
		
		coupleFrom.set(coupleFromStr);
		tripleFrom.set(coupleFrom, nbrpage);
		txt.set(tripleFrom.getPageRankTxt().toString()+"/"+tripleFrom.getNbrPage().toString());
		
		//definition du tokenizer
		String listeCoupleTo = values.toString().split("\t")[1].split(";")[1];
		StringTokenizer tokenizer = new StringTokenizer(listeCoupleTo,"#");
		
		//1er type de sortie - clé:Page valeur:pagerank
		context.write(tripleFrom.getPage(), tripleFrom.getPageRankTxt());
		
		while(tokenizer.hasMoreTokens()){
			coupleTo.set(tokenizer.nextToken());
			str=str+";"+coupleTo.getPage();
			
			//2eme type de sortie - clé:Page valeur:pagerank/nbrpage
			context.write(coupleTo.getPage() , txt);
		}
		
		//3eme type de sortie - clé:Page valeur: ;PageTo1;PageTo2...
		context.write(coupleFrom.getPage() , new Text(str));
	}


	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}
