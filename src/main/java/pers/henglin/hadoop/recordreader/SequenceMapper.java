package pers.henglin.hadoop.recordreader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by linheng on 05/11/2019.
 */
public class SequenceMapper extends Mapper<Text, BuyPhoneBean, Text, BuyPhoneBean> {
    private static Logger logger = LoggerFactory.getLogger(SequenceMapper.class);

    @Override
    protected void map(Text key, BuyPhoneBean value, Context context) throws IOException, InterruptedException {
        logger.info("key: {}, value: {}", key, value.toString());
        context.write(key, value);
    }
}
