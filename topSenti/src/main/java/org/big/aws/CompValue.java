package org.big.aws;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompValue implements WritableComparable {

    double total;
    double count;

    public CompValue(){}

    public CompValue(double total, double count){
        this.total = total;
        this.count = count;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        total = arg0.readDouble();
        count = arg0.readDouble();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeDouble(total);
        arg0.writeDouble(count);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public String toString(){
        return (total/count)+"";
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }
    
}