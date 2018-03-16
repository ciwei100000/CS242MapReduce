
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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

public class KeyWordCount {

    static public class WordMapper extends Mapper<LongWritable, Text, KeywordDocIdPair, LongWritable> {

        private final Text word = new Text();
        private final Text id = new Text();
        private final KeywordDocIdPair pair = new KeywordDocIdPair();
        private static final LongWritable ONE = new LongWritable(1);
        private static Analyzer analyzer;
        //private String field;
        //private String docID;
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

            //field = fields[4];
            //docID = fields[0];


            tokenStream = analyzer.tokenStream("content", fields[4]);
            charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                word.set(charTermAttribute.toString());
                id.set(fields[0]);
                pair.setKeyword(word);
                pair.setDocId(id);
                context.write(pair,ONE);
            }

            tokenStream.close();

        }
    }


    static public class WordReducer extends Reducer<KeywordDocIdPair, LongWritable, KeywordDocIdPair, LongWritable> {

        private LongWritable reducerValue = new LongWritable();


        @Override
        public void reduce(final KeywordDocIdPair key, final Iterable<LongWritable> values, final Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (LongWritable value:values){
                sum += value.get();
            }

            reducerValue.set(sum);

            context.write(key, reducerValue);

        }

    }

    public static class KeywordDocIdPair implements WritableComparable<KeywordDocIdPair> {

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
        public String toString() {
            return this.keyword + "\t" + this.docId;
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

    public static class KeywordDocIdPairPartioner extends Partitioner<KeywordDocIdPair, IntWritable> {

        @Override
        public int getPartition(KeywordDocIdPair pair, IntWritable intWritable, int i) {
            return Math.abs(pair.getKeyword().hashCode() % i);
        }
    }

    public static class KeywordDocIdPairGroupingComparator extends WritableComparator {

        public KeywordDocIdPairGroupingComparator() {
            super(KeywordDocIdPair.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeywordDocIdPair pair1 = (KeywordDocIdPair) a;
            KeywordDocIdPair pair2 = (KeywordDocIdPair) b;

            int compareValue = pair1.getKeyword().compareTo(pair2.getKeyword());
            if (compareValue == 0){
                compareValue = pair1.getDocId().compareTo(pair2.getDocId());
            }

            return compareValue;
        }

    }
}
