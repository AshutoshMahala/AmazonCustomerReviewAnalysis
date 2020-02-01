package big.proj.aws;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable{

    String productId;
    String productParent;
    String productCategory;

    public CompositeKey(){}

    public CompositeKey(String productId, String productParent, String productCategory) {
        this.productId = productId;
        this.productParent = productParent;
        this.productCategory = productCategory;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        productId = in.readUTF();
        productParent = in.readUTF();
        productCategory = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(productId);
        out.writeUTF(productParent);
        out.writeUTF(productCategory);
    }

    @Override
    public int compareTo(Object o) {
        CompositeKey that = (CompositeKey) o;
        if(this.getProductCategory().compareTo(that.getProductCategory())==0){
            if(this.getProductParent().compareTo(that.getProductParent())==0){
                return this.getProductId().compareTo(that.getProductId());
            }
            return this.getProductParent().compareTo(that.getProductParent());
        }
        return this.getProductCategory().compareTo(that.getProductCategory());
    }

    @Override
    public String toString(){
        return productId+"\t"+productParent+"\t"+productCategory;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductParent() {
        return productParent;
    }

    public void setProductParent(String productParent) {
        this.productParent = productParent;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }
    
}