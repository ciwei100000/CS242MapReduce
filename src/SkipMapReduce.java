
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.lang.Exception;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.io.LongWritable;
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

class SkipMapReduce {

    static public class SkipMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text word = new Text();
        private final Text id = new Text();
        private String field;
        private String docID;
        private String line;
        private String[] fields;
        private Analyzer analyzer;
        private TokenStream tokenStream;
        private CharTermAttribute charTermAttribute;

        /*
         * (non-Javadoc)
         *
         * @see
         * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
         * Mapper.Context)
         */


        /*
         * (non-Javadoc)
         *
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         * org.apache.hadoop.mapreduce.Mapper.Context)
         */

        public void map(LongWritable key, Text value, Context context)
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


                analyzer = CustomAnalyzer.builder()
                        .withTokenizer(StandardTokenizerFactory.class)
                        .addTokenFilter(StandardFilterFactory.class)
                        .addTokenFilter(LowerCaseFilterFactory.class)
                        .addTokenFilter(StopFilterFactory.class)
                        .addTokenFilter(KStemFilterFactory.class)
                        .build();

                tokenStream = analyzer.tokenStream("content", new StringReader(field));
                charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

                tokenStream.reset();

                while (tokenStream.incrementToken()) {
                    word.set(charTermAttribute.toString());
                    id.set(docID);
                    context.write(word, id);
                }

        }
    }


    static public class SkipReducer extends Reducer<Text, Text, Text, Text> {

        private final Text keyword = new Text();
        private final Text invertedlist = new Text();
        private String vocab;
        private Integer current;
        private final Map<String, Integer> docIdAndCnt = new HashMap<>();
        private final List<Map.Entry<String, Integer>> entryArrayList = new ArrayList<>();
        private boolean isfirst = true;
        private final StringBuilder toReturn = new StringBuilder();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (!entryArrayList.isEmpty()) {
                entryArrayList.clear();
            }
            if (!docIdAndCnt.isEmpty()){
                docIdAndCnt.clear();
            }
            while (values.iterator().hasNext()) {
                vocab = values.iterator().next().toString();
                current = docIdAndCnt.get(vocab);
                if (current == null) {
                    current = 0;
                }
                docIdAndCnt.put(vocab, current + 1);
            }

            entryArrayList.addAll(docIdAndCnt.entrySet());
            entryArrayList.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });


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
}
