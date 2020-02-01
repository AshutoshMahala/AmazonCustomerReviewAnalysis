package big.proj.aws;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AverageStarMapper extends Mapper<LongWritable, Text, Text, CompositeValue> {
    Text k = new Text();
    // IntWritable v = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, Text, CompositeValue>.Context context) {
        String features[] = value.toString().split("\\t");
        if (features.length != 9) {
            return;
        }
        // CompositeKey k = new CompositeKey(features[1], features[2], features[3]);
        // Text k = new Text(features[1].trim().hashCode()+"");
        k.set(features[1].trim());
        // LongWritable k = new LongWritable(features[1].trim().hashCode());
        // int stars;

        try {
            int stars = Integer.parseInt(features[4]);
            // int stars = Integer.parseInt(features[4]);
            // IntWritable v = new IntWritable();
            CompositeValue v = new CompositeValue(stars,1);
            // v.setTotal(stars);
            // v.setCount(1.0);
            // v.set(stars);
            // long stars = Long.parseLong(features[4]);
            // LongWritable v = new LongWritable(stars);
            context.write(k, v);
        } catch (NumberFormatException | IOException | InterruptedException e) {
            return;
        }

    }
}