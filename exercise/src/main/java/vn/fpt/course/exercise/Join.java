package vn.fpt.course.exercise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Join {
    // collect data post: data-badges.csv
    public static class PostMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            // type_post -> left    1
            context.write(new Text(parts[3]), new Text("left\t1" ));
        }
    }
    // collect data industry: data-industries.csv
    public static class IndustryMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] parts = record.split(",");
            // type_post -> right   industry
            context.write(new Text(parts[0]), new Text("right\t" + parts[1]));
        }
    }
    // Reduce and join by type_post => industry => count_post
    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String name = "";
            int count = 0;
            for (Text value : values)
            {
                String parts[] = value.toString().split("\t");
                if (parts[0].equals("left")) // sum number post
                {
                    count++;
                }
                else if (parts[0].equals("right")) // industry
                {
                    name = parts[1];
                }
            }
            String str = String.format("%d", count);
            if(!name.equals("")) // industry don't interested
                context.write(new Text(name), new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(Join.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // add path data-badges.csv to map data
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, PostMapper.class);
        // add path data-industries.csv to map data
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, IndustryMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
