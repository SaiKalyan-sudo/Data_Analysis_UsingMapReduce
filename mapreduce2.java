package map_reduce_codes;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class map_red_yearwise {
public static class Map extends Mapper<LongWritable, Text, Text, Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException, 
InterruptedException {
String[] line = value.toString().split(",");
String y="";
y+=line[3];
for (int i=3;i<17;i++){
y+=","+line[i];
}
context.write(new Text(line[2]),new Text(y));
}
}
public static class Reduce extends Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
int total = 0;
int t[]=new int[15];
for (Text val : values) {
String lin[]=val.toString().split(",");
for(int j=0;j<15;j++){
t[j]+=Integer.parseInt(lin[j]);;
}
}
String out="";
for(int i=0;i<15;i++){
out+=t[i]+""+'\t';
}
context.write(key, new Text(out)); 
}}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "wordcount");
job.setJarByClass(map_red_yearwise.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}}
