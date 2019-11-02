package pers.henglin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by linheng on 02/11/2019.
 */
public class HadoopJob {
    private Job job;

    public HadoopJob(Configuration conf, String jobName) throws IOException {
        this.job = Job.getInstance(conf, jobName);
    }

    public HadoopJob mapReduce(Class jar, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer){
        job.setJarByClass(jar);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        return this;
    }

    public HadoopJob mapKeyValue(Class key, Class value){
        job.setMapOutputKeyClass(key);
        job.setMapOutputValueClass(value);
        return this;
    }

    public HadoopJob reducerKeyValue(Class key, Class value){
        job.setOutputKeyClass(key);
        job.setOutputValueClass(value);
        return this;
    }

    public Job getJob(){
        return this.job;
    }
}
