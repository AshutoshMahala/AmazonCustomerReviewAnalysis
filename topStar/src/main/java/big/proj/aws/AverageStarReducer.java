package big.proj.aws;

import java.io.IOException;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageStarReducer extends Reducer<Text, CompositeValue, Text, CompositeValue> {
    long total = 0;
    long count = 0;
    @Override
    public void reduce(Text key, Iterable<CompositeValue> values,
            Reducer<Text, CompositeValue, Text, CompositeValue>.Context context) {
        
        for (CompositeValue v : values) {
                total += v.getTotal();
                count += v.getCount();
                // total += v.get();
        }
        CompositeValue val = new CompositeValue(total, count);
        // IntWritable val = new IntWritable(total);
        try {
            context.write(key, val);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}