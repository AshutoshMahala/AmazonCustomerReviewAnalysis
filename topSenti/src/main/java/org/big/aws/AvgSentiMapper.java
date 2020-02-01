package org.big.aws;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class AvgSentiMapper extends Mapper<LongWritable, Text, Text, CompValue>{
    Text k = new Text();
    // CompValue c = new CompValue();

    @Override
    public void map(LongWritable key, Text valText, Mapper<LongWritable, Text, Text, CompValue>.Context context){
        String[] feat = valText.toString().split("\\t");
        if(feat.length!=9){
            return;
        }
        
        try{
            k.set(feat[1]);
            double t = Double.parseDouble(feat[8]);
            // c.setTotal(t);
            // c.setCount(1.0);
            CompValue c = new CompValue(t,1);
            // LongWritable m = new LongWritable(1);
            context.write(k,c);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}