/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package jp.co.gihyo.wdpress.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeywordCountDriver extends Configured implements Tool { // ①

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        args = parser.getRemainingArgs();

        if (args.length != 2) {
            // ②
            System.out.printf("Usage: %s [generic options] <indir> <outdir>\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job(conf, "KeywordCount");
        job.setJarByClass(KeywordCountDriver.class);

        // ④
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
        return job.waitForCompletion(true) ? 0 : -1;
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