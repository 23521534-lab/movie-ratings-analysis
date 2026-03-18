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

public class Bai5RelevantWords {

    // ==================== MAPPER ====================
    // Input : id\ttokens\taspect\tcategory\tsentiment
    // Output: "HOTEL|sạch" -> 1
    //         "ROOMS|rộng" -> 1  ...
    public static class RelevantWordMapper
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

            if (category.isEmpty() || category.contains(" ")) return;
            if (tokensStr.isEmpty()) return;

            String[] tokens = tokensStr.split("\\s+");
            for (String token : tokens) {
                String clean = token.replaceAll("[^\\p{L}\\p{N}]", "").trim();
                if (clean.length() > 1 && !clean.matches("\\d+")) {
                    // Key: "category|word"
                    context.write(new Text(category + "|" + clean), ONE);
                }
            }
        }
    }

    // ==================== REDUCER ====================
    // Output: category|word -> count
    public static class RelevantWordReducer
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
            System.err.println("Usage: Bai5RelevantWords <input_bai1> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai5 - Relevant Words per Category");
        job.setJarByClass(Bai5RelevantWords.class);

        job.setMapperClass(RelevantWordMapper.class);
        job.setCombinerClass(RelevantWordReducer.class);
        job.setReducerClass(RelevantWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
