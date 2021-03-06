package big.proj.aws;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.util.TreeMap;
import java.util.SortedMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class TopStarMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    SortedMap<Double, String> sm;
    private int N =10;

    @Override
    public void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context){
        sm = new TreeMap<>();
    }


    @Override
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context){
        String[] tokens = value.toString().split("\\t");
        if(tokens.length!=4){
            return;
        }
        Double stars = Double.parseDouble(tokens[3]);

        sm.put(stars, tokens[0]);

        if(sm.size()> N){
            sm.remove(sm.firstKey());
        }
    }

    @Override
    public void cleanup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context){
        
        for(Double d: sm.keySet()){
            DoubleWritable dw = new DoubleWritable(d);
            Text k = new Text(sm.get(d));
            try{
                context.write(k,dw);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

}