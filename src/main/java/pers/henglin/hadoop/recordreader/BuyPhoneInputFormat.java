package pers.henglin.hadoop.recordreader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by linheng on 05/11/2019.
 */
public class BuyPhoneInputFormat extends FileInputFormat<Text, BuyPhoneBean> {
    @Override
    public RecordReader<Text, BuyPhoneBean> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        BuyPhoneRecordReader recordReader = new BuyPhoneRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }
}
