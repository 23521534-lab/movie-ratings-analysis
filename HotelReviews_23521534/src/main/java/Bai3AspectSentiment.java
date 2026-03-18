package hotel;

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

public class Bai3AspectSentiment {

    // ==================== MAPPER ====================
    // Input : id\ttokens\taspect\tcategory\tsentiment
    // Output: "GENERAL\tpositive" -> 1
    //         "QUALITY\tnegative" -> 1  ...
    public static class AspectSentimentMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] parts = line.split("\t", -1);
            if (parts.length < 5) return;

            String aspect    = parts[2].trim();
            String sentiment = parts[4].trim();

            if (aspect.isEmpty() || aspect.contains(" ")) return;
            if (!sentiment.equals("positive") && !sentiment.equals("negative")
                    && !sentiment.equals("neutral")) return;

            // Key: "aspect\tsentiment"
            context.write(new Text(aspect + "\t" + sentiment), ONE);
        }
    }

    // ==================== REDUCER ====================
    // Output: aspect\tsentiment\tcount
    public static class AspectSentimentReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // ==================== MAIN ====================
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Bai3AspectSentiment <input_bai1> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai3 - Aspect Sentiment");
        job.setJarByClass(Bai3AspectSentiment.class);

        job.setMapperClass(AspectSentimentMapper.class);
        job.setCombinerClass(AspectSentimentReducer.class);
        job.setReducerClass(AspectSentimentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
