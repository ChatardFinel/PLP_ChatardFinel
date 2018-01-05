package cs.bigdata.Question52;

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


public class PageRankInit extends Configured implements Tool {

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

		final Path pr1=new Path(outputFilePath,"pr1");
		final Path pr2=new Path(outputFilePath,"pr2");

		final ControlledJob PageRank1=pr1Job(inputFilePath,pr1);
		final ControlledJob PageRank2=pr2Job(pr1,pr2);


		final JobControl control=new JobControl("PageRankInit");
		control.addJob(PageRank1);
		PageRank2.addDependingJob(PageRank1);
		control.addJob(PageRank2);

		
		control.run();
		return control.allFinished() ? 0 : 1;

	}  




	public static void main(String[] args) throws Exception {

		PageRankInit exempleDriver = new PageRankInit();

		int res = ToolRunner.run(exempleDriver, args);

		System.exit(res);
		

	}


	private ControlledJob pr1Job(Path input,Path output) throws IOException{
		final Job job=new Job(conf,"PageRank1");

		job.setNumReduceTasks(2);

		job.setJarByClass(PageRankMapper1.class);
        job.setMapperClass(PageRankMapper1.class);
        job.setReducerClass(PageRankReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Couple.class);
        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		if (fs.exists(output)) {
			fs.delete(output,true);
		}

		return new ControlledJob(job, null);
	}

	private ControlledJob pr2Job(Path input,Path output) throws IOException {
		final Job job = new Job(conf,"PageRank2");

		job.setNumReduceTasks(2);

		job.setJarByClass(PageRankMapper2.class);
		job.setMapperClass(PageRankMapper2.class);
		job.setReducerClass(PageRankReducer2.class);

		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(TextInputFormat.class);
	    job.setOutputValueClass(TextOutputFormat.class);


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