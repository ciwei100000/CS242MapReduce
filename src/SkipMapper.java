import java.util.Collections;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.Exception;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.en.KStemFilterFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import java.io.StringReader;


public class SkipMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text word = new Text();
	private Set<String> stopWordList = new HashSet<String>();
	private BufferedReader fis;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@SuppressWarnings("deprecation")
	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {

		try {

			Path[] stopWordFiles = new Path[0];
			stopWordFiles = context.getLocalCacheFiles();
			System.out.println(stopWordFiles.toString());
		
			if (stopWordFiles != null && stopWordFiles.length > 0) {
				for (Path stopWordFile : stopWordFiles) {
					readStopWordFile(stopWordFile);
				}
			}
		} catch (IOException e) {
			System.err.println("Exception reading stop word file: " + e);

		}

	}

	/*
	 * Method to read the stop word file and get the stop words
	 */

	private void readStopWordFile(Path stopWordFile) {
		try {
			fis = new BufferedReader(new FileReader(stopWordFile.toString()));
			String stopWord = null;
			while ((stopWord = fis.readLine()) != null) {
				stopWordList.add(stopWord);
				//System.out.println(stopWord);
			}
			//stopWordList.add("and");
			//stopWordList.add("a");
		} catch (IOException ioe) {
			System.err.println("Exception while reading stop word file '"
					+ stopWordFile + "' : " + ioe.toString());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		try{
			String line = value.toString().trim().toLowerCase();
			String[] field = line.split("\t");
			
			if(field.length != 6){
				return;
			}
			
			

			String text = field[4];
		
		
		
		
			StringTokenizer tokenizer = new StringTokenizer(text);
                
			String temp = "";

        

			while (tokenizer.hasMoreTokens()) {

	

				String token = tokenizer.nextToken();
				if (stopWordList.contains(token)  ) {// || token.equals( "a")|| token.equals("and")) {
					context.getCounter(StopWordSkipper.COUNTERS.STOPWORDS)
							.increment(1L);
				//System.out.println("OK");
				} else {
					context.getCounter(StopWordSkipper.COUNTERS.GOODWORDS)
							.increment(1L);
					temp+=token+" ";
				//word.set(token);
				//context.write(word, null);
				}
			}
			temp = temp.trim();
			
			String temp2 = "";
			try{
				Analyzer analyzer = CustomAnalyzer.builder()
				                	.withTokenizer(StandardTokenizerFactory.class)
							.addTokenFilter(StandardFilterFactory.class)
							.addTokenFilter(LowerCaseFilterFactory.class)
							.addTokenFilter(StopFilterFactory.class)
							.addTokenFilter(KStemFilterFactory.class)
							.build();
				TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(temp));
				CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

				tokenStream.reset();
			
				
				while (tokenStream.incrementToken()) {
					temp2 += charTermAttribute+" ";
				}
			}catch(Exception w){
				w.printStackTrace();
			}
			
			temp2 = temp2.trim();
                        if(temp2.length()==0){ 
				word.set(field[0]+"\t"+field[1]+"\t"+field[2]+"\t"+field[3]+"\t"+temp+"\t"+field[5]);
				context.write(word,null);
         			

			}else {
			word.set(field[0]+"\t"+field[1]+"\t"+field[2]+"\t"+field[3]+"\t"+temp2+"\t"+field[5]);
			context.write(word,null);
			}
		}catch(Exception e){
			e.printStackTrace();
		}

	}
}
