package vn.fpt.course.exercise1bonus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduce {
    public static class Map extends Mapper<LongWritable, Text,Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
            // value is a line in input file
            String[] arr = value.toString().split(",");
            context.write(new Text(arr[3]+ "\t" + arr[1].substring(0,4)), new IntWritable(1));
            // key: post_type   year
            // value: 1
        }
    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
            int sum=0;
            for(IntWritable x: values)
            {
                sum+=x.get();
            }
            context.write(key, new IntWritable(sum));
            // output key  count
        }
    }
    public static void main(String[] args) throws Exception {
        // create configure for job
        Configuration conf= new Configuration();
        Job job = new Job(conf,"Count Post Each Year");
        job.setJarByClass(MapReduce.class);
        // set class for map phase
        job.setMapperClass(Map.class);
        // set class for suffle phase
        job.setCombinerClass(Reduce.class);
        // set class for reduce phase
        job.setReducerClass(Reduce.class);
        // type key of output: post_type + '\t' + year
        job.setOutputKeyClass(Text.class);
        // type value of output: sum
        job.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[1]);
        //Configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //deleting the output path automatically from hdfs so that we don't have to delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
