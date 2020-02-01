package sentiment.customer.review;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.net.URI;

/**
 * Sentiment Analysis MapReduce
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String[] files = {"pos-words.txt", "neg-words.txt", "stop-words.txt"};
        System.out.println("Starting!");
        Instant start = Instant.now();
        Configuration conf = new Configuration();

        String confPathStr = args[0];
        String inPathStr = args[1];
        String outPathStr = args[2];

        // System.out.println(Paths.get(System.getProperty("java.class.path")).toString());

        try{
            Job job = Job.getInstance(conf, "Sentiment Analysis");

            job.setJarByClass(App.class);

            for(String file: files){
                URI uri = new Path(confPathStr+file).toUri();
                // System.out.println(uri.toString());
                job.addCacheFile(uri);   
            }

            job.setMapperClass(SentimentMapper.class);
            // job.setCombinerClass(SentimentReducer.class);
            job.setReducerClass(SentimentReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(CompositeKey.class);
            job.setMapOutputValueClass(CompositeValue.class);

            job.setOutputKeyClass(CompositeKey.class);
            job.setOutputValueClass(CompositeValue.class);

            FileInputFormat.addInputPath(job, new Path(inPathStr));
            FileOutputFormat.setOutputPath(job, new Path(outPathStr));

            // Delete the result folder if present
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(outPathStr),true);

            int n = job.waitForCompletion(true) ? 0 : 1;

            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

            System.exit(n);
        }catch(IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
