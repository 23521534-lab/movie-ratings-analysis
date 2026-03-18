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

public class Bai4SentimentWords {

    // ==================== MAPPER ====================
    // Input : id\ttokens\taspect\tcategory\tsentiment
    // Output: "HOTEL|positive|khách" -> 1
    //         "HOTEL|negative|bẩn"   -> 1  ...
    public static class SentimentWordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] parts = line.split("\t", -1);
            if (parts.length < 5) return;

            String tokensStr = parts[1].trim();
            String category  = parts[3].trim();
            String sentiment = parts[4].trim();

            // Chỉ xét positive và negative
            if (!sentiment.equals("positive") && !sentiment.equals("negative")) return;
            if (category.isEmpty() || category.contains(" ")) return;
            if (tokensStr.isEmpty()) return;

            String[] tokens = tokensStr.split("\\s+");
            for (String token : tokens) {
                String clean = token.replaceAll("[^\\p{L}\\p{N}]", "").trim();
                if (clean.length() > 1 && !clean.matches("\\d+")) {
                    // Key: "category|sentiment|word"
                    context.write(new Text(category + "|" + sentiment + "|" + clean), ONE);
                }
            }
        }
    }

    // ==================== REDUCER ====================
    // Output: category|sentiment|word -> count
    public static class SentimentWordReducer
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
            System.err.println("Usage: Bai4SentimentWords <input_bai1> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai4 - Sentiment Words per Category");
        job.setJarByClass(Bai4SentimentWords.class);

        job.setMapperClass(SentimentWordMapper.class);
        job.setCombinerClass(SentimentWordReducer.class);
        job.setReducerClass(SentimentWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
