package org.big.aws;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.time.Duration;
import java.time.Instant;


public class App {
    public static void main( String[] args )
    {
        System.out.println("Starting!");
        Instant start = Instant.now();

        Configuration conf1 = new Configuration();

        String input = args[0];
        String intermidiate = args[1];
        String output = args[2];

        try{
            Job job1 = Job.getInstance(conf1,"sentiment Avg");

            job1.setJarByClass(App.class);

            job1.setMapperClass(AvgSentiMapper.class);
            job1.setReducerClass(AvgSentiReducer.class);

            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(CompValue.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(CompValue.class);

            FileInputFormat.addInputPath(job1,new Path(input));
            FileOutputFormat.setOutputPath(job1,new Path(intermidiate));

            // Delete result folder if exists
            FileSystem fs = FileSystem.get(conf1);
            fs.delete(new Path(intermidiate), true);

            int n = job1.waitForCompletion(true)? 0: 1;

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

        }catch(Exception e){
            e.printStackTrace();
        }


        try{

            Job job2 = Job.getInstance(conf1,"sentiment Avg");

            job2.setJarByClass(App.class);

            job2.setMapperClass(TopSentiMap.class);
            job2.setReducerClass(TopSentiReduce.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job2,new Path(intermidiate));
            FileOutputFormat.setOutputPath(job2,new Path(output));

            // Delete result folder if exists
            FileSystem fs = FileSystem.get(conf1);
            fs.delete(new Path(output), true);

            int n = job2.waitForCompletion(true)? 0: 1;

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

            System.exit(n);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
