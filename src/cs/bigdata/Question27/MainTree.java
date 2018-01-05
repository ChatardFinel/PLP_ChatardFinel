//meme architecture que dans la classe CompterLigneFile

package cs.bigdata.Question27;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import cs.bigdata.Question27.Tree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
public class MainTree {

	public static void main(String[] args) throws IOException {
		
		//fichier source
		String localSrc = "/Users/Cassandre/eclipse-workspace/PLPYearHeightTree/sources/arbres.csv";
		
		//ouvrir le fichier
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			//lecture ligne par ligne
			String line = br.readLine();
					
			while (line !=null){
			
				//traiter la ligne en cours
				System.out.println(Tree.DisplayYearHeight(line));
				//aller a la ligne suivante
				line = br.readLine();
			}
		}
					
		finally{
			//fermer le fichier source
			in.close();
			fs.close();
		}

	}
}