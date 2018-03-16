import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortedIndex {

    static public class SortMapper extends Mapper<LongWritable, Text, KeywordCountPair, Text> {

        private final Text word = new Text();
        private final Text mapperValue = new Text();
        private final LongWritable count = new LongWritable();

        private final KeywordCountPair pair = new KeywordCountPair();
        private final StringBuilder stringBuilder = new StringBuilder();

        private String line;
        private String[] fields;

        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {

            line = value.toString();
            fields = line.split("\t");

            if (stringBuilder.length() != 0) {
                stringBuilder.setLength(0);
            }

            if (fields.length != 3) {
                return;
            }

            if (!(StringUtils.isNumeric(fields[1]) && StringUtils.isNumeric(fields[2]) && Long.valueOf(fields[1]) > 100000)) {
                return;
            }


            word.set(fields[0]);
            count.set(Long.valueOf(fields[2]));

            pair.setCount(count);
            pair.setKeyword(word);


            stringBuilder.append(fields[1]).append(":").append(fields[2]);

            mapperValue.set(stringBuilder.toString());

            context.write(pair, mapperValue);
        }
    }


    static public class SortReducer extends Reducer<KeywordCountPair, Text, Text, Text> {

        private Text reducerValue = new Text();
        private StringBuilder stringBuilder = new StringBuilder();

        @Override
        public void reduce(final KeywordCountPair key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            if (stringBuilder.length() != 0) {
                stringBuilder.setLength(0);
            }

            for (Text value : values) {
                stringBuilder.append(value);
                stringBuilder.append(";");
            }


            reducerValue.set(stringBuilder.toString());

            context.write(key.getKeyword(), reducerValue);

        }

    }

    public static class KeywordCountPair implements WritableComparable<SortedIndex.KeywordCountPair> {

        private Text keyword = new Text();
        private LongWritable count = new LongWritable();


        @Override
        public int compareTo(SortedIndex.KeywordCountPair pair) {
            int compareValue = this.keyword.compareTo(pair.getKeyword());
            if (compareValue == 0) {
                compareValue = -1 * this.count.compareTo(pair.getCount());
            }

            return compareValue;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            keyword.write(dataOutput);
            count.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            keyword.readFields(dataInput);
            count.readFields(dataInput);
        }

        @Override
        public String toString() {
            return this.keyword + "\t" + this.count;
        }

        @Override
        public int hashCode() {
            return this.keyword.hashCode() * 1990 + this.count.hashCode() * 926;
        }

        public boolean equals(SortedIndex.KeywordCountPair pair) {
            return this.keyword.equals(pair.getKeyword()) && this.count.equals(pair.getCount());

        }

        public void setKeyword(Text keyword) {
            this.keyword = keyword;
        }

        public void setCount(LongWritable count) {
            this.count = count;
        }

        public Text getKeyword() {
            return this.keyword;
        }

        public LongWritable getCount() {
            return this.count;
        }


    }

    public static class KeywordCountPairPartioner extends Partitioner<SortedIndex.KeywordCountPair, IntWritable> {

        @Override
        public int getPartition(SortedIndex.KeywordCountPair pair, IntWritable intWritable, int i) {
            return Math.abs(pair.getKeyword().hashCode() % i);
        }
    }

    public static class KeywordCountPairGroupingComparator extends WritableComparator {

        public KeywordCountPairGroupingComparator() {
            super(SortedIndex.KeywordCountPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SortedIndex.KeywordCountPair pair1 = (SortedIndex.KeywordCountPair) a;
            SortedIndex.KeywordCountPair pair2 = (SortedIndex.KeywordCountPair) b;

            return pair1.getKeyword().compareTo(pair2.getKeyword());

        }

    }
}
