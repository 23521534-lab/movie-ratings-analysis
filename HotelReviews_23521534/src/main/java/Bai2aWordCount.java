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

public class Bai2aWordCount {

    // ==================== MAPPER ====================
    // Input : id\ttokens\taspect\tcategory\tsentiment
    // Output: word -> 1
    public static class WordCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] parts = line.split("\t", -1);
            if (parts.length < 2) return;

            String tokensStr = parts[1].trim();
            if (tokensStr.isEmpty()) return;

            String[] tokens = tokensStr.split("\\s+");
            for (String token : tokens) {
                String clean = token.replaceAll("[^\\p{L}\\p{N}]", "").trim();
                // Lọc số thuần và từ ngắn
                if (clean.length() > 1 && !clean.matches("\\d+")) {
                    context.write(new Text(clean), ONE);
                }
            }
        }
    }

    // ==================== REDUCER ====================
    // Input : word -> [1, 1, 1, ...]
    // Output: word -> totalCount
    public static class WordCountReducer
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
            System.err.println("Usage: Bai2aWordCount <input_bai1> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai2a - Word Count");
        job.setJarByClass(Bai2aWordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class); // dùng Combiner để tối ưu
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
