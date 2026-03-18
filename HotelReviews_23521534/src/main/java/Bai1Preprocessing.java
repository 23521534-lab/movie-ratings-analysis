package hotel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Bai1Preprocessing {

    // ==================== MAPPER ====================
    public static class PreprocessMapper
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        private Set<String> stopwords = new HashSet<>();
        private boolean firstLine = true;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Đọc stopwords từ Distributed Cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream("stopwords.txt"), "UTF-8"));
                String word;
                while ((word = br.readLine()) != null) {
                    word = word.trim();
                    if (!word.isEmpty()) stopwords.add(word);
                }
                br.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Bỏ qua header
            if (line.startsWith("id;")) return;

            // Parse CSV (dấu ; là separator)
            String[] parts = line.split(";", -1);
            if (parts.length < 5) return;

            String id        = parts[0].trim();
            String review    = parts[1].trim();
            String aspect    = parts[2].trim();
            String category  = parts[3].trim();
            String sentiment = parts[4].trim();

            if (id.equals("id") || review.isEmpty()) return;

            // Bước 1: Lowercase
            String lowerReview = review.toLowerCase();

            // Bước 2: Tách từ theo khoảng trắng
            String[] tokens = lowerReview.split("\\s+");

            // Bước 3: Lọc stopwords + làm sạch dấu câu
            List<String> filtered = new ArrayList<>();
            for (String token : tokens) {
                String clean = token.replaceAll("[.,!?;:\"'()\\[\\]{}/]", "").trim();
                if (clean.length() > 1 && !stopwords.contains(clean)) {
                    filtered.add(clean);
                }
            }

            // Output: id\ttokens\taspect\tcategory\tsentiment
            String output = id + "\t" + String.join(" ", filtered)
                          + "\t" + aspect + "\t" + category + "\t" + sentiment;
            context.write(NullWritable.get(), new Text(output));
        }
    }

    // ==================== REDUCER (Identity) ====================
    public static class IdentityReducer
            extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
    }

    // ==================== MAIN ====================
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Bai1Preprocessing <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1 - Preprocessing");
        job.setJarByClass(Bai1Preprocessing.class);

        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(IdentityReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Thêm stopwords vào Distributed Cache
        job.addCacheFile(new URI(args[2] + "#stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
