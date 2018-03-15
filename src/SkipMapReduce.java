import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.Exception;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
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

public class SkipMapReduce {

	static public class SkipMapper extends Mapper<LongWritable, Text, Text, Text> {

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
		protected void setup(Context context) throws java.io.IOException,
				InterruptedException {

			try {

				Path[] stopWordFiles = context.getLocalCacheFiles();

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

			try {
				String line = value.toString().trim().toLowerCase();
				String[] field = line.split("\t");

				if (field.length != 6) {
					return;
				}


				String text = field[4];


				StringTokenizer tokenizer = new StringTokenizer(text);

				String temp = "";


				while (tokenizer.hasMoreTokens()) {


					String token = tokenizer.nextToken();
					if (stopWordList.contains(token)) {// || token.equals( "a")|| token.equals("and")) {
						context.getCounter(StopWordSkipper.COUNTERS.STOPWORDS)
								.increment(1L);
						//System.out.println("OK");
					} else {
						context.getCounter(StopWordSkipper.COUNTERS.GOODWORDS)
								.increment(1L);
						temp += token + " ";
						//word.set(token);
						//context.write(word, null);
					}
				}
				temp = temp.trim();

				String temp2 = "";
				try {
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
						temp2 += charTermAttribute + " ";
					}
				} catch (Exception w) {
					w.printStackTrace();
				}

				temp2 = temp2.trim();

				String ID = field[0];
				String UserID = field[1];
				String ScreenName = field[2];
				String UserName = field[3];
				String TweetEntity = field[5];


				try {
					if (temp2.length() == 0) {

						//				String NewLines = "";


						String IndexLine = UserID + "\t" + ScreenName + "\t" + UserName + "\t" + temp + "\t" + TweetEntity;
						String[] IndexFields = IndexLine.split("\t");


						for (int i = 0; i < IndexFields.length - 1; i++) {
							String Indexfield = IndexFields[i];
							StringTokenizer Tokn = new StringTokenizer(Indexfield);

							//					String NewFields = "";

							while (Tokn.hasMoreTokens()) {
								String Indexword = Tokn.nextToken();
								//						NewFields = NewFields + Indexword + " ";
								word.set(Indexword);
								context.write(word, new Text(ID));
							}
						}

						String[] tweetentity = TweetEntity.split(";;");

						if (tweetentity.length == 4) {
							String[] retweetid = tweetentity[0].split(",");
							String[] hashtags = tweetentity[1].split(",");
							String[] urls = tweetentity[2].split(",");
							String[] mentions = tweetentity[3].split(",");

							for (int i = 1; i < retweetid.length; i++) {
								word.set(retweetid[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < hashtags.length; i++) {
								word.set(hashtags[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < urls.length; i++) {
								word.set(urls[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < mentions.length; i++) {
								word.set(mentions[i]);
								context.write(word, new Text(ID));
							}
						}

					} else {


						String IndexLine = UserID + "\t" + ScreenName + "\t" + UserName + "\t" + temp2 + "\t" + TweetEntity;
						String[] IndexFields = IndexLine.split("\t");


						for (int i = 0; i < IndexFields.length - 1; i++) {
							String Indexfield = IndexFields[i];
							StringTokenizer Tokn = new StringTokenizer(Indexfield);

							//					String NewFields = "";

							while (Tokn.hasMoreTokens()) {
								String Indexword = Tokn.nextToken();
								//						NewFields = NewFields + Indexword + " ";
								word.set(Indexword);
								context.write(word, new Text(ID));
							}
						}

						String[] tweetentity = TweetEntity.split(";;");

						if (tweetentity.length == 4) {
							String[] retweetid = tweetentity[0].split(",");
							String[] hashtags = tweetentity[1].split(",");
							String[] urls = tweetentity[2].split(",");
							String[] mentions = tweetentity[3].split(",");

							for (int i = 1; i < retweetid.length; i++) {
								word.set(retweetid[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < hashtags.length; i++) {
								word.set(hashtags[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < urls.length; i++) {
								word.set(urls[i]);
								context.write(word, new Text(ID));
							}

							for (int i = 1; i < mentions.length; i++) {
								word.set(mentions[i]);
								context.write(word, new Text(ID));
							}
						}
					}
				} catch (Exception x) {
					x.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}


	static public class SkipReducer extends Reducer<Text, Text, Text, Text> {


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


			Map<String, Integer> wordAndCnt = new HashMap<>();
			while (values.iterator().hasNext()) {
				String vocab = values.iterator().next().toString();
				Integer current = wordAndCnt.get(vocab);
				if (current == null) {
					current = 0;
				}
				wordAndCnt.put(vocab, current + 1);
			}


			List<Map.Entry<String, Integer>> entryArrayList = new ArrayList<>(wordAndCnt.entrySet());
			Collections.sort(entryArrayList, Comparator.comparing(Map.Entry::getKey));
			//		for (Map.Entry<String, Integer> entry : entryArrayList) {
			//			System.out.println(entry.getKey() + " - " + entry.getValue());
			//		}


			boolean isfirst = true;
			StringBuilder toReturn = new StringBuilder();
			for (Map.Entry<String, Integer> entry : entryArrayList) {
				if (!isfirst) {
					toReturn.append(";");
				}
				isfirst = false;
				toReturn.append(entry.getKey()).append(":").append(entry.getValue());
			}


			//		TreeMap<String, Integer> treeMap = new TreeMap<>();

			context.write(new Text(key), new Text(toReturn.toString()));

		}
	}
}