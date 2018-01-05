package cs.bigdata.Question51;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;


public class TFIDF extends Configured implements Tool {

	private Configuration conf;
	private FileSystem fs;


	public int run(String[] args) throws Exception {

		if (args.length != 2) {

			System.out.println("Usage: [input] [output]");

			System.exit(-1);

		}

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		conf = getConf();
		fs = FileSystem.newInstance(conf);

		final Path tfidf1=new Path(outputFilePath,"tfidf1");
		final Path tfidf2=new Path(outputFilePath,"tfidf2");
		final Path tfidf3=new Path(outputFilePath,"tfidf3");
		
		final ControlledJob TFIDF1=tfidf1Job(inputFilePath,tfidf1);
		final ControlledJob TFIDF2=tfidf2Job(tfidf1,tfidf2);
		final ControlledJob TFIDF3=tfidf3Job(tfidf2,tfidf3);

		final JobControl control=new JobControl("TFIDF");
		control.addJob(TFIDF1);
		TFIDF2.addDependingJob(TFIDF1);
		control.addJob(TFIDF2);
		TFIDF3.addDependingJob(TFIDF1);
		TFIDF3.addDependingJob(TFIDF2);
		control.addJob(TFIDF3);

		control.run();
		return control.allFinished() ? 0 : 1;

	}  




	public static void main(String[] args) throws Exception {

		TFIDF exempleDriver = new TFIDF();

		int res = ToolRunner.run(exempleDriver, args);

		System.exit(res);
		

	}


	private ControlledJob tfidf1Job(Path input,Path output) throws IOException{
		final Job job=new Job(conf,"TFIDF1");

		job.setNumReduceTasks(4);

		job.setJarByClass(TFIDFMapper1.class);
		job.setMapperClass(TFIDFMapper1.class);
		job.setReducerClass(TFIDFReducer1.class);

		job.setMapOutputKeyClass(MotDoc.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (fs.exists(output)) {
			fs.delete(output,true);
		}

		return new ControlledJob(job, null);
	}

	private ControlledJob tfidf2Job(Path input,Path output) throws IOException {
		final Job job = new Job(conf,"TFIDF2");

		job.setNumReduceTasks(4);

		job.setJarByClass(TFIDFMapper2.class);
		job.setMapperClass(TFIDFMapper2.class);
		job.setReducerClass(TFIDFReducer2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MotNbrMots.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (fs.exists(output)) {
			fs.delete(output,true);
		}

		return new ControlledJob(job, null);
	}

	private ControlledJob tfidf3Job(Path input,Path output) throws IOException {
		final Job job = new Job(conf,"TFIDF3");

		job.setNumReduceTasks(4);

		job.setJarByClass(TFIDFMapper3.class);
		job.setMapperClass(TFIDFMapper3.class);
		job.setReducerClass(TFIDFReducer3.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (fs.exists(output)) {
			fs.delete(output,true);
		}

		return new ControlledJob(job, null);
	}

	
	



}