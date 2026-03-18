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

public class Bai2bcStats {

    // ==================== MAPPER ====================
    // Input : id\ttokens\taspect\tcategory\tsentiment
    // Output: "CATEGORY:<cat>" -> 1
    //         "ASPECT:<asp>"   -> 1
    public static class StatsMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] parts = line.split("\t", -1);
            if (parts.length < 5) return;

            String aspect   = parts[2].trim();
            String category = parts[3].trim();

            // Chỉ lấy giá trị hợp lệ (chữ hoa, không khoảng trắng)
            if (!category.isEmpty() && category.equals(category.toUpperCase())
                    && !category.contains(" ")) {
                context.write(new Text("CATEGORY:" + category), ONE);
            }

            if (!aspect.isEmpty() && aspect.equals(aspect.toUpperCase())
                    && !aspect.contains(" ")) {
                context.write(new Text("ASPECT:" + aspect), ONE);
            }
        }
    }

    // ==================== REDUCER ====================
    public static class StatsReducer
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
            System.err.println("Usage: Bai2bcStats <input_bai1> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai2bc - Category & Aspect Stats");
        job.setJarByClass(Bai2bcStats.class);

        job.setMapperClass(StatsMapper.class);
        job.setCombinerClass(StatsReducer.class);
        job.setReducerClass(StatsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
