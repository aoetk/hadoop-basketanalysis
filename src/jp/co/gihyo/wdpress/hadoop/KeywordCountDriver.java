/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeywordCountDriver extends Configured implements Tool { // ①

    @Override
    public int run(String[] args) throws Exception {
/*        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", getClass().getSimpleName());
            return -1;
        }*/
        if (args.length != 3) {
            System.out.printf("Usage: %s [generic options] <indir> <intermediate outdir> <outdir>\n", getClass().getSimpleName());
            return -1;
        }
        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = new Job(getConf(), "KeywordCount"); // ②
        job.setJarByClass(KeywordCountDriver.class);

        // ④
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, intermediatePath);

        // ⑤
        job.setMapperClass(KeywordMapper.class);
        job.setReducerClass(SumReducer.class);

        // ⑥
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // ⑦
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ⑧
//        return job.waitForCompletion(true) ? 0 : -1;
        boolean ret = job.waitForCompletion(true);
        if (!ret) {
            return -1;
        }

        Job secondJob = new Job(getConf(), "Sort");
        secondJob.setJarByClass(KeywordCountDriver.class);

        FileInputFormat.addInputPath(secondJob, intermediatePath);
        FileOutputFormat.setOutputPath(secondJob, outputPath);

        secondJob.setMapperClass(CountMapper.class);
        secondJob.setReducerClass(OutputReducer.class);

        secondJob.setMapOutputKeyClass(IntWritable.class);
        secondJob.setPartitionerClass(CountPartitioner.class);
        secondJob.setMapOutputValueClass(Text.class);
        secondJob.setSortComparatorClass(InverseComparator.class);

        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);

        return secondJob.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        // ⑨
        int exitCode = ToolRunner.run(new KeywordCountDriver(), args);
        System.exit(exitCode);
    }

}

class KeywordMapper extends Mapper<LongWritable, Text, Text, IntWritable> { // ①
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); // ②
        String[] records = line.split("\t"); // ③
        if (records.length == 3) {
            context.write(new Text(records[2]), new IntWritable(1)); // ④
        }
    }
}

class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> { // ①
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get(); // ②
        }
        context.write(key, new IntWritable(count));
    }
}

class CountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] records = line.split("\t"); // ①
        if (records.length == 2) {
            context.write(new IntWritable(Integer.parseInt(records[1])), new Text(records[0])); // ②
        }
    }
}

class OutputReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}

class InverseComparator extends WritableComparator {
    protected InverseComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return - super.compare(a, b);
    }
}

class CountPartitioner extends Partitioner<IntWritable, Text> { // ①
    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) { // ②
        if (numPartitions > 1 && key.get() > 20) {
            return 1;
        }
        return 0;
    }
}
