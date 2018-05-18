import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import java.util.Date;
import java.util.*;
import org.apache.hadoop.mapreduce.Job;
import java.util.Arrays;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.util.Scanner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class HadoopSort {
public static class Map_Hadoop 
extends Mapper<Object, Text, Text, Text> 
{
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	String str = value.toString();
	String ok = str.substring(0, 10);	
	String ov = str.substring(10);
	Text kt = new Text(ov);
	Text kv = new Text(ok);
	context.write( kv,  kt); 
}
}
public static class Reduce_Hadoop extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> hadoop_values,Context context) throws IOException, InterruptedException {		
	Text ok = key;
	Text ov = new Text();
	for (Text val : hadoop_values) {
		ov = val;
	}
	context.write(ok, ov);
}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
Configuration conf = new Configuration();
long st;
st	= System.currentTimeMillis();
Job job = Job.getInstance(conf, "HadoopSort - Proton");
job.setReducerClass(Reduce_Hadoop.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setJarByClass(HadoopSort.class);
job.setJobName("HADOOP");
job.setMapperClass(Map_Hadoop.class);		
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
if (job.waitForCompletion(true)) {
	long et ;
	et= System.currentTimeMillis();	
	double hadoop_time_elapsed = ((double) et - st) / 1000.0;
	System.out.println("Total time to sort data on hadoop: " + hadoop_time_elapsed + " seconds");
}
}
}









































