/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author aoetakashi
 */
public class KeywordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        JobConf jobConf = new JobConf(getConf(), KeywordCountDriver.class);
        jobConf.setJobName("KeywordCount");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        jobConf.setMapperClass(KeywordMapper.class);
        jobConf.setReducerClass(SumReducer.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(IntWritable.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KeywordCountDriver(), args);
        System.exit(exitCode);
    }

}
class KeywordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    private Text keyword = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] records = line.split("\t");
        if (records.length == 3) {
            keyword.set(records[2]);
            output.collect(keyword, ONE);
        }
    }

}

class SumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable keywordCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }
        keywordCount.set(count);
        output.collect(key, keywordCount);
    }

}