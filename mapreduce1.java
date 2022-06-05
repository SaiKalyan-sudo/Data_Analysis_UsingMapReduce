package map_reduce_codes;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class map_red_total {
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
IntWritable one = new IntWritable(1);
Text new_key=new Text();
public void map(LongWritable key, Text value, Context context) throws IOException, 
InterruptedException {
String[] line = value.toString().split(",");
context.write(new Text(line[0]),new IntWritable(Integer.parseInt(line[3])));
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
int total = 0;
for (IntWritable val : values) {
total += val.get();
}
context.write(key, new IntWritable(total));
} 
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "wordcount");
job.setJarByClass(map_red_total.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
}
