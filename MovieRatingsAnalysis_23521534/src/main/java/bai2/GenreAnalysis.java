package bai2;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenreAnalysis {

    static Map<String,String> movieGenres = new HashMap<>();

    public static class RatingMapper extends Mapper<Object,Text,Text,FloatWritable>{

        protected void setup(Context context)throws IOException{
            Path[] files=context.getLocalCacheFiles();
            BufferedReader br=new BufferedReader(new FileReader(files[0].toString()));
            String line;
            while((line=br.readLine())!=null){
                String[] p=line.split(",",3);
                if(p.length>=3)
                    movieGenres.put(p[0],p[2]);
            }
            br.close();
        }

        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{

            String[] f=value.toString().split(",");
            if(f.length>=3){

                String genres=movieGenres.get(f[1]);
                float rating=Float.parseFloat(f[2]);

                if(genres!=null)
                    for(String g:genres.split("\\|"))
                        context.write(new Text(g),new FloatWritable(rating));
            }
        }
    }

    public static class ReducerAvg extends Reducer<Text,FloatWritable,Text,Text>{
        public void reduce(Text key,Iterable<FloatWritable> values,Context context)
                throws IOException,InterruptedException{

            float sum=0;
            int c=0;

            for(FloatWritable v:values){
                sum+=v.get();
                c++;
            }

            context.write(key,new Text(String.format("%.2f (%d)",sum/c,c)));
        }
    }

    public static void main(String[] args)throws Exception{

        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"genre");

        job.setJarByClass(GenreAnalysis.class);

        job.addCacheFile(new Path(args[1]).toUri());

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(ReducerAvg.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}