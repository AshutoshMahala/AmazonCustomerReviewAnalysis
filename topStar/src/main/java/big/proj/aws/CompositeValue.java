package big.proj.aws;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements WritableComparable{

    private long total;
    private long count;
 
    public CompositeValue(){}

    public CompositeValue(long total, long count){
            this.total  = total;
            this.count = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        total = in.readLong();
        count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(total);
        out.writeLong(count);
    }

    @Override
    public int compareTo(Object o) {
        // CompositeValue that = (CompositeValue) o;
        // if(this.getAverage()>that.getAverage()){
        //     return 1;
        // }else if(this.getAverage()<that.getAverage()){
        //     return -1;
        // }
        return 0;
    }

    @Override
    public String toString(){
        return getTotal()+"\t"+getCount()+"\t"+getAverage();
    }

    // public long getStarRating() {
    //     return starRating;
    // }

    // public void setStarRating(long starRating) {
    //     this.starRating = starRating;
    // }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverage() {
        // long avg = (1.0 *total)/(count*1.0);
        return 1.0*total/count;
    }

}