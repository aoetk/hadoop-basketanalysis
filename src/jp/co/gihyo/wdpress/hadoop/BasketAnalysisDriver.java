/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author aoetakashi
 */
public class BasketAnalysisDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage: %s [generic options] <indir> <intermediate outdir> <outdir>\n", getClass().getSimpleName());
            return -1;
        }
        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 1段目のジョブ
        Configuration firstConf = new Configuration();
        Job firstJob = new Job(firstConf);
        firstJob.setJobName("BuildCollocation");

        FileInputFormat.addInputPath(firstJob, inputPath);
        FileOutputFormat.setOutputPath(firstJob, intermediatePath);

        firstJob.setMapperClass(KeyMapper.class);
        firstJob.setReducerClass(KeywordPairReducer.class);

        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(Text.class);

        firstJob.setPartitionerClass(UserIdPartitioner.class);
        firstJob.setGroupingComparatorClass(GroupComparator.class);

        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);

        boolean ret = firstJob.waitForCompletion(true);
        if (!ret) {
            return -1;
        }

        // 2段目のジョブ
        Configuration secondConf = new Configuration();
        Job secondJob = new Job(secondConf, "CountCollocation");

        FileInputFormat.addInputPath(secondJob, intermediatePath);
        FileOutputFormat.setOutputPath(secondJob, outputPath);

        secondJob.setMapperClass(KeywordPairMapper.class);
        secondJob.setReducerClass(SumReducer.class);

        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(IntWritable.class);

        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);

        return secondJob.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BasketAnalysisDriver(), args);
        System.exit(exitCode);
    }

}

class KeyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text idTimePair = new Text();
    private Text timeKeywordPair = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] records = line.split("\t");
        if (records.length == 3) {
            idTimePair.set(records[0] + "#" + records[1]);
            timeKeywordPair.set(records[1] + "#" + records[2]);
            context.write(idTimePair, timeKeywordPair);
        }
    }
}

class KeywordPairReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static final long WINDOW = 120L;

    private static final IntWritable ONE = new IntWritable(1);

    private Text keywordPair = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> keywordList = new ArrayList<String>();
        for (Text value : values) {
            keywordList.add(value.toString());
        }

        for (int i = 0; i < keywordList.size(); i++) {
            String basePair = keywordList.get(i);
            String[] baseRecords = basePair.split("#");
            long baseTime = Long.parseLong(baseRecords[0]);

            for (int j = i + 1; j < keywordList.size(); j++) {
                String timeAndKeywordPair = keywordList.get(j);
                String[] records = timeAndKeywordPair.split("#");
                if (baseRecords[1].equals(records[1])) {
                    continue;
                }
                long diff = Long.parseLong(records[0]) - baseTime;
                if (diff > WINDOW) {
                    break;
                }
                keywordPair.set(baseRecords[1] + "#" + records[1]);
                context.write(keywordPair, ONE);
            }
        }
    }

}

class UserIdPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String userId = key.toString().split("#")[0];
        return (userId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String keyA = ((Text) a).toString().split("#")[0];
        String keyB = ((Text) b).toString().split("#")[0];
        return keyA.compareTo(keyB);
    }

}

class KeywordPairMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] records = line.split("\t");
        if (records.length == 2) {
            context.write(new Text(records[0]), new IntWritable(Integer.parseInt(records[1])));
        }
    }
}