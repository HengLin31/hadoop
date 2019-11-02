package pers.henglin.hadoop.buyphone;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static pers.henglin.hadoop.Constant.REGEX_COLS_SPLIT_SYMBOL;

/**
 * Created by linheng on 02/11/2019.
 */
public class CustomerBean implements Writable {
    protected static final String CUSTOMER = "customer.txt";
    protected static final String PRODUCT = "product.txt";
    protected static final String PRICE = "price.txt";

    private String dataType;

    private String customerId;
    private String productId;
    private String productName;
    private double productPrice;


    public CustomerBean() {
        super();
    }

    public CustomerBean(String dataType, String customerId, String productId, String productName, double productPrice) {
        super();
        this.dataType = dataType;
        this.customerId = customerId;
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.dataType);
        out.writeUTF(this.customerId);
        out.writeUTF(this.productName);
        out.writeDouble(this.productPrice);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dataType = in.readUTF();
        this.customerId = in.readUTF();
        this.productName = in.readUTF();
        this.productPrice = in.readDouble();
    }

    @Override
    public String toString() {
        return customerId + REGEX_COLS_SPLIT_SYMBOL + productName + REGEX_COLS_SPLIT_SYMBOL + productPrice;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
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
}
