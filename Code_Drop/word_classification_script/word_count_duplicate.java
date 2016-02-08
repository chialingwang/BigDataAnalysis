
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
	
public class word_count_duplicate {
	
		public static void main(String[] args) throws IOException
		{
			try {
				
				String Word ;
			    FileReader fr1 = new FileReader(args[0]);
				BufferedReader br1 = new BufferedReader(fr1);
			
					
			
			
					while ((Word = br1.readLine()) != null){
							String line = Word.toString();
							String[] pair = line.split("\t");
							int count = Integer.parseInt(pair[1]);
							for (int i=0 ; i< count ; i++)
								System.out.println(""+pair[0]+"\t");
								//writer.println(""+pair[0]+"\t");					
						
						
					}
			
					
					
					//writer.close();
			    			    			
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


