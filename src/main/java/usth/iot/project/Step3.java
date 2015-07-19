package usth.iot.project;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
 * combine the user table and the cooccurence table (only single record)
 */

public class Step3 {
	public static final String INPUT_PATH1 = "hdfs://crxy1:9000/recommand/step1/part*";
	public static final String INPUT_PATH2 = "hdfs://crxy1:9000/recommand/step2/part*";
	public static final String OUT_PATH = "hdfs://crxy1:9000/recommand/step3";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH1), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, Step3.class.getSimpleName());
		job.setJarByClass(Step3.class);
		MultipleInputs.addInputPath(job, new Path(INPUT_PATH1), TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, new Path(INPUT_PATH2), TextInputFormat.class, MyMapper2.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("[\t,]");
			for (int i = 101; i <= 107; i++) {
				for (int j = 1; j < split.length; j++) {
					String[] split2 = split[j].split(":");
					String k2 = i + ":" + split2[0] + ":" + split[0];
					//long v2 = Long.parseLong(split2[1]);
					double v2 = Double.parseDouble(split2[1]);
					context.write(new Text(k2), new DoubleWritable(v2));
				}
			}
		}
	}

	public static class MyMapper2 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			for (int i = 1; i <= 5; i++) {
				String k2 = split[0] + ":" + i;
				Double v2 = Double.parseDouble(split[1]);
				context.write(new Text(k2), new DoubleWritable(v2));
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2s,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			double result = 1;
			for (DoubleWritable single : v2s) {
				count++;
				result = result * single.get();
				if (count > 1) {
					context.write(k2, new DoubleWritable(result));
				}
			}
		}
	}
}
