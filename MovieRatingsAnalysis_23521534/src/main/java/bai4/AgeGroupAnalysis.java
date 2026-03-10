package bai4;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AgeGroupAnalysis {

    public static class JoinMapper extends Mapper<Object,Text,Text,Text>{

        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{

            String file=((FileSplit)context.getInputSplit()).getPath().getName();
            String[] f=value.toString().split(",");

            if(file.contains("ratings") && f.length>=3)
                context.write(new Text(f[0]),new Text("R:"+f[2]));

            else if(file.contains("users") && f.length>=3)
                context.write(new Text(f[0]),new Text("U:"+f[2]));
        }
    }

    public static class ReducerJoin extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{

            int age=-1;
            List<Float> ratings=new ArrayList<>();

            for(Text v:values){

                String s=v.toString();

                if(s.startsWith("U:"))
                    age=Integer.parseInt(s.substring(2));
                else
                    ratings.add(Float.parseFloat(s.substring(2)));
            }

            if(age!=-1){

                String g=(age<=18)?"0-18":
                        (age<=35)?"18-35":
                        (age<=50)?"35-50":"50+";

                for(Float r:ratings)
                    context.write(new Text(g),new Text(String.valueOf(r)));
            }
        }
    }

    public static class AvgReducer extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{

            float sum=0;
            int c=0;

            for(Text v:values){
                sum+=Float.parseFloat(v.toString());
                c++;
            }

            context.write(key,new Text(String.format("%.2f (%d)",sum/c,c)));
        }
    }

    public static void main(String[] args)throws Exception{

        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"age");

        job.setJarByClass(AgeGroupAnalysis.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),
                TextInputFormat.class,JoinMapper.class);

        MultipleInputs.addInputPath(job,new Path(args[1]),
                TextInputFormat.class,JoinMapper.class);

        job.setReducerClass(ReducerJoin.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path(args[3]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}