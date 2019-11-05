package pers.henglin.hadoop.recordreader;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static pers.henglin.hadoop.Constant.REGEX_COLS_SPLIT_SYMBOL;

/**
 * Created by linheng on 05/11/2019.
 */
public class BuyPhoneBean implements Writable {
    private String productName;
    private double productPrice;
    private String customerIds;

    public BuyPhoneBean(){
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.productName);
        out.writeDouble(this.productPrice);
        out.writeUTF(this.customerIds);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.productName = in.readUTF();
        this.productPrice = in.readDouble();
        this.customerIds = in.readUTF();
    }

    @Override
    public String toString() {
        return  productName + REGEX_COLS_SPLIT_SYMBOL + productPrice + REGEX_COLS_SPLIT_SYMBOL + customerIds;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(double productPrice) {
        this.productPrice = productPrice;
    }

    public String getCustomerIds() {
        return customerIds;
    }

    public void setCustomerIds(String customerIds) {
        this.customerIds = customerIds;
    }
}
