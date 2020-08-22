package models;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class JobWrapper<OUTPUT_VALUE_CLASS extends WritableComparable> extends Configured implements Tool {

    private String outputPath;
    private String inputPath;
    private Class<OUTPUT_VALUE_CLASS> outputValueClass;
    private Class<Mapper<LongWritable,Text,Text,OUTPUT_VALUE_CLASS>> mapperClass;
    private Class<Reducer<Text, OUTPUT_VALUE_CLASS, Text, Text>> reducerClass;
    private String jobName;

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        jobName = strings[0];
        inputPath = strings[1];
        outputPath = strings[2];
        System.out.println("param1");
        outputValueClass = (Class<OUTPUT_VALUE_CLASS>) Class.forName(strings[3]);
        System.out.println(outputValueClass);
        mapperClass = (Class<Mapper<LongWritable, Text, Text, OUTPUT_VALUE_CLASS>>) Class.forName(strings[4]);
        System.out.println(mapperClass);
        reducerClass = (Class<Reducer<Text, OUTPUT_VALUE_CLASS, Text, Text>>) Class.forName(strings[5]);
        System.out.println(reducerClass);
        conf.set("mapred.textoutputformat.separator",",");
        System.out.println("param5");
        if(outputValueClass.equals(RegressionVariablesWrapper.class)){
            System.out.println("param6");
            conf.setStrings("regression.key.concat.indexes",strings[6].split(","));
            conf.setInt("regression.x.variable.index",Integer.parseInt(strings[7]));
            conf.setInt("regression.y.variable.index",Integer.parseInt(strings[8]));
        }
        else{
            System.out.println("param7");
            conf.setStrings("key.concat.indexes",strings[6].split(","));
            conf.setInt("out.index",Integer.parseInt(strings[7]));
            conf.set("separator",strings[8]);
            conf.setInt("date.index",Integer.parseInt(strings[9]));
            conf.setStrings("key.concat.date.indexes",strings[10].split(","));
        }
        System.out.println("param8");
        Job job = new Job(conf, jobName);
        System.out.println("param9");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.out.println("param10");
        job.setMapperClass(mapperClass);
        System.out.println("param11");
        job.setReducerClass(reducerClass);
        System.out.println("param12");
        job.setOutputKeyClass(Text.class);
        System.out.println("param13");
        job.setOutputValueClass(outputValueClass);
        System.out.println("param14");
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
