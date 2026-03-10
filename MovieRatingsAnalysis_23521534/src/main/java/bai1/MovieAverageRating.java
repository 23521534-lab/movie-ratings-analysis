package bai1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MovieAverageRating {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split(",");
            if (f.length >= 3)
                context.write(new Text(f[1]), new Text("R:" + f[2]));
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split(",",3);
            if (f.length >= 2)
                context.write(new Text(f[0]), new Text("T:" + f[1]));
        }
    }

    public static class ReducerAvg extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;
            String title = "";

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("T:"))
                    title = s.substring(2);
                else {
                    sum += Float.parseFloat(s.substring(2));
                    count++;
                }
            }

            if (count >= 5) {
                float avg = sum / count;
                context.write(new Text(title),
                        new Text(String.format("%.2f (%d ratings)", avg, count)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg rating");

        job.setJarByClass(MovieAverageRating.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),
                TextInputFormat.class,RatingMapper.class);

        MultipleInputs.addInputPath(job,new Path(args[1]),
                TextInputFormat.class,MovieMapper.class);

        job.setReducerClass(ReducerAvg.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}