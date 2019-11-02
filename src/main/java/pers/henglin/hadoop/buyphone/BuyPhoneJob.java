package pers.henglin.hadoop.buyphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import pers.henglin.hadoop.HadoopJob;

import java.io.IOException;

/**
 * Created by linheng on 02/11/2019.
 */
public class BuyPhoneJob {
    private static final String JOB_NAME = "buy-phone";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] {"/Users/linheng/Documents/test/testdata", "/Users/linheng/Documents/test/output3"};
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2){
            System.err.println("Usage: " + JOB_NAME + " <in> <out>");
            System.exit(2);
        }

        Job job = new HadoopJob(conf, JOB_NAME)
                .mapReduce(BuyPhoneJob.class, BuyPhoneMapper.class, BuyPhoneReducer.class)
                .mapKeyValue(Text.class, CustomerBean.class)
                .reducerKeyValue(Text.class, Text.class)
                .getJob();

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
