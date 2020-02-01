package sentiment.customer.review;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class SentimentMapper extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {

    private SentimentAnalyzer analyzer;
    private static final int MIN_WORD_LIMIT = 20;
    private CompositeKey compKey;
    private CompositeValue compValue;

    public void setup(Mapper<LongWritable, Text, CompositeKey, CompositeValue>.Context context){
        URI[] uris;
        try {
            uris = context.getCacheFiles();
            System.out.println(uris[0].toString());
            analyzer = new SentimentAnalyzer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, CompositeKey, CompositeValue>.Context context) {
        String features[] = value.toString().split("\\t");
        if (key.get()==0 || features.length != 15) {
            return;
        }
        int count = features[13].split("\\s").length;
        int stars, helpfulVotes, totalVotes;
        try {
            stars = Integer.parseInt(features[7]);
            helpfulVotes = Integer.parseInt(features[8]);
            totalVotes = Integer.parseInt(features[9]);
        } catch (NumberFormatException nfe) {
            return;
        }
        if (count >= MIN_WORD_LIMIT) {
            double sentiScore = analyzer.getSentimentScore(features[13]);
            // double sentiScore = 0.5;
            compKey = new CompositeKey(features[2], features[3], features[4], features[6]);
            compValue = new CompositeValue(stars, count, helpfulVotes, totalVotes, sentiScore);
            try {
                context.write(compKey, compValue);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}