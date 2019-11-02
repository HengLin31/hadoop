package pers.henglin.hadoop.buyphone;

import com.google.common.base.Joiner;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static pers.henglin.hadoop.Constant.*;
import static pers.henglin.hadoop.buyphone.CustomerBean.*;

/**
 * Created by linheng on 02/11/2019.
 */
public class BuyPhoneReducer extends Reducer<Text, CustomerBean, Text, Text> {
    private static Logger logger = LoggerFactory.getLogger(BuyPhoneReducer.class);

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<CustomerBean> values, Context context) throws IOException, InterruptedException {
        List<CustomerBean> customers = new ArrayList<>();
        String productName = ENPTY_STRING;
        double productPrice = 0d;
        for(CustomerBean customerBean:values){
            switch(customerBean.getDataType()){
                case CUSTOMER:
                    addCustomer(customers, customerBean);
                    break;
                case PRODUCT:
                    productName = customerBean.getProductName();
                    break;
                case PRICE:
                    productPrice = customerBean.getProductPrice();
                    break;
                default:
                    logger.warn("It can't find this data type: {}", customerBean.getDataType());
                    return;
            }
        }
        writeResult2(context, customers, productName, productPrice);
    }

    private void addCustomer(List<CustomerBean> customers, CustomerBean customer){
        CustomerBean temp = new CustomerBean();
        try {
            BeanUtils.copyProperties(temp, customer);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        customers.add(temp);
    }

    private void writeResult2(Context context, List<CustomerBean> customers, String productName, double productPrice) throws IOException, InterruptedException {
        if(customers.isEmpty()){
            return;
        }
        List<String> customerIds = new ArrayList<>();
        for(CustomerBean customer:customers){
            customer.setProductName(productName);
            customer.setProductPrice(productPrice);
            customerIds.add(customer.getCustomerId());
        }
        this.outputKey.set(productName + REGEX_COLS_SPLIT_SYMBOL + productPrice);
        this.outputValue.set(Joiner.on(REGEX_DATA_JOIN_SPLIT_SYMBOL).join(customerIds));
        context.write(this.outputKey, this.outputValue);
    }
}
