package big.proj.aws;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopStarReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
            Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) {
        for (DoubleWritable value : values) {
            try {
                context.write(key, value);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}