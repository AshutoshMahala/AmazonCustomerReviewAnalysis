package big.proj.aws;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println("Starting!");
        Instant start = Instant.now();
        Configuration conf1 = new Configuration();

        String inPath = args[0];
        String midPath = args[1];
        String outPath = args[2];
        try{
            Job job1 = Job.getInstance(conf1, "Avg Star");

            job1.setJarByClass(App.class);

            job1.setMapperClass(AverageStarMapper.class);
            // job1.setCombinerClass(AverageStarReducer.class);
            job1.setReducerClass(AverageStarReducer.class);

            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(CompositeValue.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(CompositeValue.class);

            FileInputFormat.addInputPath(job1, new Path(inPath));
            FileOutputFormat.setOutputPath(job1, new Path(midPath));

            // Delete the result folder if present
            FileSystem fs = FileSystem.get(conf1);
            fs.delete(new Path(midPath),true);

            int n = job1.waitForCompletion(true) ? 0 : 1;

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

        }catch(Exception e){
            e.printStackTrace();
        }

        try{

            Job job2 = Job.getInstance(conf1,"star top");

            job2.setJarByClass(App.class);

            job2.setMapperClass(TopStarMap.class);
            job2.setReducerClass(TopStarReduce.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job2,new Path(midPath));
            FileOutputFormat.setOutputPath(job2,new Path(outPath));

            // Delete result folder if exists
            FileSystem fs = FileSystem.get(conf1);
            fs.delete(new Path(outPath), true);

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
