package pers.henglin.hadoop.recordreader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by linheng on 05/11/2019.
 */
public class SequenceReducer extends Reducer<Text, BuyPhoneBean, Text, BuyPhoneBean> {
    @Override
    protected void reduce(Text key, Iterable<BuyPhoneBean> values, Context context) throws IOException, InterruptedException {
        for(BuyPhoneBean value:values){
            context.write(key, value);
        }
    }
}
