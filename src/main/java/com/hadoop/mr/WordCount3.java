package com.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * ClassName: WordCount3
 * Description:
 * Date: 2016/6/29 21:43
 *
 * @author SAM SHO
 * @version V1.0
 */
public class WordCount3 extends Configured implements Tool {
    private final static IntWritable one = new IntWritable();
    private final static IntWritable result = new IntWritable();
    private static Text word = new Text();

    public static void main(String[] args) throws Exception {
        int mr = ToolRunner.run(new Configuration(), new WordCount3(), args);
        System.exit(mr);
    }

    public int run(String[] args) throws Exception {

        // 创建Job，设置配置和 JOb的名称
        Job job = Job.getInstance(this.getConf(), "wordCount");

        // 设置Job 运行的类
        job.setJarByClass(WordCount2.class);

        // 设置 Mapper 与 Reducer的类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // 设置输入文件的目录和输出文件的目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 输出的结果的 key 与 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 提交Job，等待运行结果，并在客户端显示运行结果
        boolean isSucess = job.waitForCompletion(true);

        // 结束程序
        return isSucess ? 0 : -1;
    }


    /**
     * Map 区域
     */
    static class WordMapper extends Mapper<IntWritable, Text, Text, IntWritable> {

        /**
         * @param key     每一行的位置 所以是 IntWritable 类型
         * @param value   解析后的每行的单词，Text 类型
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

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
    static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key
         * @param values  已经 shuffling 过了
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values) {
                num += value.get();
            }
            result.set(num);
            context.write(key, result);
        }
    }

}
