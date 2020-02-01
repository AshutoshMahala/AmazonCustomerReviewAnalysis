package org.big.aws;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgSentiReducer extends Reducer<Text, CompValue, Text, CompValue> {
    double sum = 0.0;
    double count = 0.0;

    @Override
    public void reduce(Text key, Iterable<CompValue> values,
            Reducer<Text, CompValue, Text, CompValue>.Context context) {
        for (CompValue cv : values) {
            sum += cv.getTotal();
            count += cv.getCount();
        }
        // long total=0;
        // Iterator<CompValue> itr = values.iterator();
        // while(itr.hasNext()){
        //     total+= itr.next().getTotal();
            
        // }
        CompValue value = new CompValue(sum, count);
        // LongWritable value = new LongWritable(total);
        try {
            context.write(key, value);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}