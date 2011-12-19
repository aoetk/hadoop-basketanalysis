/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
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
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }
        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // 1段目のジョブ
        JobConf firstJobConf = new JobConf(getConf(), BasketAnalysisDriver.class);
        firstJobConf.setJobName("BuildCollocation");

        FileInputFormat.addInputPath(firstJobConf, inputPath);
        FileOutputFormat.setOutputPath(firstJobConf, intermediatePath);

        firstJobConf.setMapperClass(KeyMapper.class);
        firstJobConf.setReducerClass(KeywordPairReducer.class);

        firstJobConf.setMapOutputKeyClass(Text.class);
        firstJobConf.setMapOutputValueClass(Text.class);

        firstJobConf.setPartitionerClass(UserIdPartitioner.class);
        firstJobConf.setOutputValueGroupingComparator(GroupComparator.class);

        firstJobConf.setOutputKeyClass(Text.class);
        firstJobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(firstJobConf);

        // 2段目のジョブ
        JobConf secondJobConf = new JobConf(getConf(), BasketAnalysisDriver.class);
        secondJobConf.setJobName("CountCollocation");

        secondJobConf.setInputFormat(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(secondJobConf, intermediatePath);
        FileOutputFormat.setOutputPath(secondJobConf, outputPath);

        secondJobConf.setMapperClass(ToIntMapper.class);
        secondJobConf.setReducerClass(SumReducer.class);

        secondJobConf.setMapOutputKeyClass(Text.class);
        secondJobConf.setMapOutputValueClass(IntWritable.class);

        secondJobConf.setOutputKeyClass(Text.class);
        secondJobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(secondJobConf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BasketAnalysisDriver(), args);
        System.exit(exitCode);
    }

}
class KeyMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

    private Text idTimePair = new Text();
    private Text timeKeywordPair = new Text();

    @Override
    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] records = line.split("\t");
        if (records.length == 3) {
            idTimePair.set(records[0] + "#" + records[1]);
            timeKeywordPair.set(records[1] + "#" + records[2]);
            output.collect(idTimePair, timeKeywordPair);
        }
    }

}

class KeywordPairReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

    private static final long WINDOW = 120L;

    private static final IntWritable ONE = new IntWritable(1);

    private Text keywordPair = new Text();

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        List<String> keywordList = new ArrayList<String>();
        while (values.hasNext()) {
            keywordList.add(values.next().toString());
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
                output.collect(keywordPair, ONE);
            }
        }
    }

}


class ToIntMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

    private IntWritable count = new IntWritable();

    @Override
    public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        count.set(Integer.parseInt(value.toString()));
        output.collect(key, count);
    }

}

class UserIdPartitioner implements Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String userId = key.toString().split("#")[0];
        return (userId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public void configure(JobConf job) {
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
