package vn.fpt.course.exercise.bonus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {
    public static class PostYearMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t");
            context.write(new Text(parts[0]), new Text("left\t" + parts[1] + "\t" + parts[2]));// type_post => left    year    count
        }
    }

    public static class IndustryMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("right\t" + parts[1])); // type_post-> right    industry
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            int count = 0;
            List<String> queue = new ArrayList<String>();
            for (Text value : values) {
                String parts[] = value.toString().split("\t");
                if (parts[0].equals("left")) // post : year count
                {
                    String data = parts[1] + "\t" + parts[2];
                    if (name.equals("")) { // don't have data of industry => wait
                        queue.add(data);
                    } else // have industry data
                        context.write(new Text(name), new Text(data));
                } else if (parts[0].equals("right")) { // industry
                    name = parts[1];
                    for (String data : queue) { // have industry data => write result
                        context.write(new Text(name), new Text(data));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(Join.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // get output of previous mapreduce job
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostYearMapper.class);
        // get data-industries.csv
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IndustryMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
