import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemmer;
import org.apache.lucene.analysis.en.KStemmer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.EnglishStemmer;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.lucene.analysis.en.KStemFilter;


public class Tokenized_and_Stem {
	
	public static class TokenizeMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable one  = new IntWritable (1);
	private Text word = new Text() ;
		
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		/*replaceAll("*","").*/
		
		line = line.replaceAll("##@@##",",").replaceAll("\"","");
		
		line = line.replaceAll("\"\"\"",",").replaceAll("<a.*</a>", "").replaceAll("(?i)<.*P.*>", "").replaceAll("(?i)<.*u.*>", "").replaceAll("\n", " ");
		//System.out.println(line);
		String line_low = line.toLowerCase();
		
		
		StringTokenizer tokenizer = new StringTokenizer(line_low, " \t\n\r\f,.:;?![]()\"=$-/");
		while (tokenizer.hasMoreTokens()) {
			
			String patten = "^'*(\\w+)'*$";

			String token = tokenizer.nextToken().replaceAll(patten, "$1");
			
			//Stem //
			PorterStemmer stemmer = new PorterStemmer();
			stemmer.setCurrent(token);
			stemmer.stem();
			//End of Stem //
			

			
			word.set(stemmer.getCurrent());
			context.write(word, one);
		
		}
	} 
	}
	
	public static class TokenizeReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values){
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));	
	}
	}
	
		
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Tokenized_and_Stem <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();

		job.setJarByClass(Tokenized_and_Stem.class);
		job.setJobName("Tokenized_and_Stem");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TokenizeMapper.class);
		job.setReducerClass(TokenizeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		
		
	}
}

