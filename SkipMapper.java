

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


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
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
                
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
		word.set(temp);
		context.write(word,null);
	}
}
