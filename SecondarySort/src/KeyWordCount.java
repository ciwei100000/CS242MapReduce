
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class KeyWordCount {

    static public class WordMapper extends Mapper<LongWritable, Text, KeywordDocIdPair, IntWritable> {

        private final Text word = new Text();
        private final Text id = new Text();
        private final KeywordDocIdPair pair = new KeywordDocIdPair();
        private static final IntWritable ONE = new IntWritable(1);
        private static Analyzer analyzer;
        private String field;
        private String docID;
        private String line;
        private String[] fields;
        private TokenStream tokenStream;
        private CharTermAttribute charTermAttribute;



        @Override
        protected void setup(Context context) throws IOException {

            analyzer = CustomAnalyzer.builder()
                    .withTokenizer(StandardTokenizerFactory.class)
                    .addTokenFilter(StandardFilterFactory.class)
                    .addTokenFilter(LowerCaseFilterFactory.class)
                    .addTokenFilter(StopFilterFactory.class)
                    .addTokenFilter(KStemFilterFactory.class)
                    .build();
        }

        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {

            line = value.toString();
            fields = line.split("\t");

            if (fields.length != 6) {
                return;
            }

            if (!(StringUtils.isNumeric(fields[0]) && StringUtils.isNumeric(fields[1]) && Long.valueOf(fields[1]) > 100000)) {
                return;
            }

            field = fields[4];
            docID = fields[0];


            tokenStream = analyzer.tokenStream("content", new StringReader(field));
            charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                word.set(charTermAttribute.toString());
                id.set(docID);
                pair.setKeyword(word);
                pair.setDocId(id);
                context.write(pair,ONE);
            }

        }
    }


    static public class WordReducer extends Reducer<KeywordDocIdPair, IntWritable, Text, Text> {

        private final Text keyword = new Text();
        private final Text invertedlist = new Text();
        private KeywordDocIdPair pair;
        private final StringBuilder toReturn = new StringBuilder();
        private boolean isfirst;


        @Override
        public void reduce(final KeywordDocIdPair key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value:values){
                sum += value.get();
            }


            isfirst = true;
            if(toReturn.length() != 0){
                toReturn.setLength(0);
            }

            for (Map.Entry<String, Integer> entry : entryArrayList) {
                if (!isfirst) {
                    toReturn.append(";");
                }
                isfirst = false;
                toReturn.append(entry.getKey()).append(":").append(entry.getValue());
            }


            //		TreeMap<String, Integer> treeMap = new TreeMap<>();

            keyword.set(key);
            invertedlist.set(toReturn.toString());

            context.write(keyword, invertedlist);

        }

    }

    static class KeywordDocIdPair implements WritableComparable<KeywordDocIdPair> {

        private Text keyword = new Text();
        private Text docId = new Text();


        @Override
        public int compareTo(KeywordDocIdPair pair){
            int compareValue = this.keyword.compareTo(pair.getKeyword());
            if (compareValue == 0) {
                compareValue = this.docId.compareTo(pair.getDocId());
            }

            return compareValue;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException{
            keyword.write(dataOutput);
            docId.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException{
            keyword.readFields(dataInput);
            docId.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            return this.keyword.hashCode()*1990+ this.docId.hashCode()*926;
        }

        public boolean equals(KeywordDocIdPair pair) {
            return this.keyword.equals(pair.getKeyword()) && this.docId.equals(pair.getDocId());

        }

        public void setKeyword(Text keyword) {
            this.keyword = keyword;
        }

        public void setDocId(Text docId) {
            this.docId = docId;
        }

        public Text getKeyword(){
            return this.keyword;
        }

        public Text getDocId(){
            return this.docId;
        }
    }
}
