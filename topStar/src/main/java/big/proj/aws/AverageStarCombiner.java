package big.proj.aws;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageStarCombiner extends Reducer<Text, LongWritable, Text, CompositeValue> {

    public void Reduce(Text key, Iterable<LongWritable> values,
            Reducer<Text, LongWritable, Text, CompositeValue>.Context context) {
        long total = 0;
        long count = 0;
        for (LongWritable value : values) {
                total += value.get();
                count += 1 ;
        }
        CompositeValue val = new CompositeValue(total, count);
        try {
            context.write(key, val);
        } catch (IOException | InterruptedException e) {
        }
    }
}