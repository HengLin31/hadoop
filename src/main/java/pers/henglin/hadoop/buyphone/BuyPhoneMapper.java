package pers.henglin.hadoop.buyphone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.henglin.hadoop.Constant;

import java.io.IOException;

import static pers.henglin.hadoop.Constant.ENPTY_STRING;
import static pers.henglin.hadoop.buyphone.CustomerBean.*;

/**
 * Created by linheng on 02/11/2019.
 */
public class BuyPhoneMapper extends Mapper<LongWritable, Text, Text, CustomerBean> {
    private static Logger logger = LoggerFactory.getLogger(BuyPhoneMapper.class);

    private String dataType;
    private CustomerBean customerBean = new CustomerBean();
    private Text outputKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.dataType = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] cols = record.split(Constant.REGEX_COLS_SPLIT_SYMBOL);
        this.customerBean.setDataType(this.dataType);
        switch(this.dataType){
            case CUSTOMER:
                setCustomer(cols, this.outputKey, this.customerBean);
                break;
            case PRODUCT:
                setProduct(cols, this.outputKey, this.customerBean);
                break;
            case PRICE:
                setPrice(cols, this.outputKey, this.customerBean);
                break;
            default:
                logger.warn("It can't find this data type: {}", this.dataType);
                return;
        }
        context.write(this.outputKey, this.customerBean);
    }

    // record: customerId, productId
    private void setCustomer(String[] cols, Text key, CustomerBean customerBean){
        String customerId = cols[0];
        String productId = cols[1];
        customerBean.setCustomerId(customerId);
        customerBean.setProductId(productId);
        customerBean.setProductName(ENPTY_STRING);
        customerBean.setProductPrice(0d);
        key.set(productId);
    }

    // record: productId, productName
    private void setProduct(String[] cols, Text key, CustomerBean customerBean){
        String productId = cols[0];
        String productName = cols[1];
        customerBean.setCustomerId(ENPTY_STRING);
        customerBean.setProductId(productId);
        customerBean.setProductName(productName);
        customerBean.setProductPrice(0d);
        key.set(productId);
    }

    // record: productId, productPrice
    private void setPrice(String[] cols, Text key, CustomerBean customerBean){
        String productId = cols[0];
        double productPrice = Double.parseDouble(cols[1]);
        customerBean.setCustomerId(ENPTY_STRING);
        customerBean.setProductId(productId);
        customerBean.setProductName(ENPTY_STRING);
        customerBean.setProductPrice(productPrice);
        key.set(productId);
    }
}
