package sentiment.customer.review;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class SentimentReducer extends Reducer<CompositeKey, CompositeValue, CompositeKey, CompositeValue> {

    public void Reduce(CompositeKey key, Iterable<CompositeValue> values,
            Reducer<CompositeKey, CompositeValue, CompositeKey, CompositeValue>.Context context) {

        for (CompositeValue value : values) {
            try {
                context.write(key, value);
            } catch (IOException | InterruptedException e) {
            }
        }
    }
}