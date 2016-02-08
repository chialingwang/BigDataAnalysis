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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;
import org.apache.lucene.analysis.en.KStemmer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.lucene.analysis.en.KStemFilter;


public class Stop_Word {
	public static void main(String[] args) throws IOException
	{
		try {
			
		    File directory = new File(args[0]);
		    //String filename[] = directory.list();
		    File[] files = directory.listFiles();
		    FileReader fr1 = new FileReader(args[1]);
			BufferedReader br1 = new BufferedReader(fr1);
		    for (File file : files) {
		    	if (file.isFile()) {
		            
		    	FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);

				
				String strNum ; 
				String StopWord ;
				int count = 0;
				Hashtable hash = new Hashtable();
				
				PrintWriter writer = new PrintWriter(args[2], "UTF-8");
		
		
				while ((StopWord = br1.readLine()) != null){
					StringTokenizer tokenizer = new StringTokenizer(StopWord, " \t");
					while (tokenizer.hasMoreTokens()) {
						String token = tokenizer.nextToken();
						//Stem //
						PorterStemmer stemmer = new PorterStemmer();
						stemmer.setCurrent(token);
						stemmer.stem();
						//End of Stem //
						
						count = count+1;
						hash.put( stemmer.getCurrent(), count);
					
					}
					
				}
		
				while ((strNum = br.readLine()) != null){
					String line = strNum.toString();
					String[] pair = line.split("\t");
					if(!hash.containsKey(pair[0])){
					System.out.println(line);
					writer.println(line);
		
					
					}
				}
				writer.close();
		    }
		    }
		
	} 
	

/*		System.out.println(strNum);
		String[] tokens = strNum.split("-|,|\\ ");
		for (String token:tokens) {
			   System.out.println(token);
		}
		System.out.println("The first "+str2+" index in line is "+strNum.indexOf(str2)+"");
		}*/
		catch(IOException ex) {
			System.err.println("Input File missing!");
			System.err.println("Usage: Stop_Word <input path> <Stop word list> <output path>");
			ex.printStackTrace();
		 }
		
	}
	
}
