package pers.henglin.hadoop.recordreader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static pers.henglin.hadoop.Constant.REGEX_COLS_SPLIT_SYMBOL;

/**
 * Created by linheng on 05/11/2019.
 */
public class BuyPhoneRecordReader extends RecordReader<Text, BuyPhoneBean> {
    private static Logger logger = LoggerFactory.getLogger(BuyPhoneRecordReader.class);

    private LineRecordReader lineRecordReader;

    private Text key = new Text();
    private BuyPhoneBean value = new BuyPhoneBean();

    private Text lineValue;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        this.lineRecordReader = new LineRecordReader();
        this.lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(lineRecordReader.nextKeyValue()){
            lineValue = lineRecordReader.getCurrentValue();
            byte[] line = lineValue.getBytes();
            int lineLen = lineValue.getLength();
            String data = new String(line, 0, lineLen);
            setKeyValue(this.key, this.value, data.split(REGEX_COLS_SPLIT_SYMBOL));
        }else{
            return false;
        }
        return true;
    }

    private void setKeyValue(Text key, BuyPhoneBean value, String[] cols){
        if(cols.length < 3){
            logger.warn("this cols[] length < 3");
            return;
        }
        String productName = cols[0];
        double productPrice = Double.parseDouble(cols[1]);
        String customerIds = cols[2];

        key.set(new Text(productName));

        value.setProductName(productName);
        value.setProductPrice(productPrice);
        value.setCustomerIds(customerIds);
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BuyPhoneBean getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.lineRecordReader.close();
    }
}
