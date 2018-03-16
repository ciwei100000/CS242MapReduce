
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SecondarySortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = getConf();

        if (args.length != 3){
            throw new IllegalArgumentException("Usage: SecondarySortDriver <Input-Path> <Temp-Path> <Output-Path>");
        }

        Job job1 = Job.getInstance(conf,"KeyWordCount");
        job1.setJarByClass(SecondarySortDriver.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job1,inputPath);
        FileOutputFormat.setOutputPath(job1,outputPath);

        job1.setOutputKeyClass(KeyWordCount.KeywordDocIdPair.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);

        job1.setMapperClass(KeyWordCount.WordMapper.class);
        job1.setReducerClass(KeyWordCount.WordReducer.class);
        job1.setPartitionerClass(KeyWordCount.KeywordDocIdPairPartioner.class);
        job1.setGroupingComparatorClass(KeyWordCount.KeywordDocIdPairGroupingComparator.class);

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf,"SortIndex");
        job2.setJarByClass(SecondarySortDriver.class);

        inputPath = new Path(args[1]);
        outputPath = new Path(args[2]);
        FileInputFormat.setInputPaths(job2,inputPath);
        FileOutputFormat.setOutputPath(job2,outputPath);

        job2.setOutputKeyClass(SortedIndex.KeywordCountPair.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);

        job2.setMapperClass(SortedIndex.SortMapper.class);
        job2.setReducerClass(SortedIndex.SortReducer.class);
        job2.setPartitionerClass(SortedIndex.KeywordCountPairPartioner.class);
        job2.setGroupingComparatorClass(SortedIndex.KeywordCountPairGroupingComparator.class);

        boolean status =job2.waitForCompletion(true);

        return status? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int returnStatus = ToolRunner.run(new SecondarySortDriver(),args);
        System.exit(returnStatus);
    }
}
