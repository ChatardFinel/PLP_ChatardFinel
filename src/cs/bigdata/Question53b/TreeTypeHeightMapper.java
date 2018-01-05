package cs.bigdata.Question53b;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class TreeTypeHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	private final static FloatWritable taille = new FloatWritable();
	private Text type = new Text();
	// Overriding of the map method
	@Override
	
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException{
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
	
		//Le type est dans la 3eme colonne du fichier csv
		String lineType = valE.toString().split(";")[2];
		
		//La taille est dans la 7eme colonne du fichier csv
		String lineTaille = valE.toString().split(";")[6];
		
		StringTokenizer tokenizerType = new StringTokenizer(lineType);
		StringTokenizer tokenizerTaille = new StringTokenizer(lineTaille);
		
		while(tokenizerTaille.hasMoreTokens()){
			type.set(tokenizerType.nextToken());
			
			//on a définit la taille comme un float, mais en entree c'est un texte
			try {
				taille.set(Float.parseFloat(tokenizerTaille.nextToken()));
				context.write(type, taille);
			}
			
			//il faut etre sur de pouvoir la convertir, pour la ligne avec le titre ce n'est pas possible par exemple
			catch (NumberFormatException e) {
				FloatWritable zero=new FloatWritable(0);
				context.write(type, zero);
			}
			
		}
    }

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}
