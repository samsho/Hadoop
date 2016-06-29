package com.hadoop.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.StringTokenizer;

/**
 * ClassName: WordCount
 * Description: 自定义 MR 的 WordCount
 * Date: 2016/6/4 13:37
 *
 * @author SAM SHO
 * @version V1.0
 */
public class WordCount {

    private final static LongWritable one = new LongWritable(1);
    private final static LongWritable result = new LongWritable();
    private static Text word = new Text();

    /**
     * Map 区域
     */
    static class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * @param key     每一行的位置 LongWritable 类型
         * @param value   解析后的每行的单词，Text 类型
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取每行数据的值
            String lineValue = value.toString();
            // 进行分割
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            // 遍历
            while (stringTokenizer.hasMoreElements()) {
                // 获取每个值
                String wordValue = stringTokenizer.nextToken();
                // 设置map 输出的值
                word.set(wordValue);
                // 上下文输出map 的 key 和 value
                context.write(word, one);//有就写一次
            }
        }
    }

    /**
     * Reducer 区域
     **/
    static class WordReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * @param key
         * @param values  已经 shuffling 过了
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (LongWritable value : values) {
                num += value.get();
            }
            result.set(num);
            context.write(key, result);
        }
    }

    /**
     * Client 区域
     **/
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputDir = "hdfs://master:9000/hadoop-study/mr/input/wordcount.txt";
        String outDir = "hdfs://master:9000/hadoop-study/mr/output/wordcount2";
        // 获取配置
        final Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://master:9000");
        String ticketCachePath = configuration.get("hadoop.security.kerberos.ticket.cache.path");
        UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, "hadoop");
        Job job = (Job) ugi.doAs(new PrivilegedExceptionAction() {
            public Job run() throws Exception {
                return Job.getInstance(configuration, "wordCount");
            }
        });
        // 设置Job 运行的类
        job.setJarByClass(WordCount.class);
        // 设置 Mapper 与 Reducer的类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        // 设置输入文件的目录和输出文件的目录
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        // 输出的结果的 key 与 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 提交Job，等待运行结果，并在客户端显示运行结果
        boolean isSuccess = job.waitForCompletion(true);
        // 结束程序
        System.exit(isSuccess ? 0 : -1);
    }
}
