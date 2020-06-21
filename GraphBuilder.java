package exp3;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {
  /** 得到输出 <FromPage, <1.0 ,ToPage1,ToPage2...>> */
  public static class GraphBuilderMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String valueString=value.toString();
      //输出格式<URL,1.0\t出边列表>
      //其中1.0为初始pr值
      context.write(new Text(valueString.split("\t")[0]), new Text("1.0\t"+valueString.split("\t")[1]));
    }
  }

  public static class GraphBuilderReducer extends
      Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", "hdfs://localhost:9000");
      Job job1 =Job.getInstance(conf,"Graph builder");
      job1.setJarByClass(GraphBuilder.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      job1.setMapperClass(GraphBuilderMapper.class);
      job1.setReducerClass(GraphBuilderReducer.class);
//      FileInputFormat.addInputPath(job1, new Path("input"));
//      FileOutputFormat.setOutputPath(job1, new Path("mid_result"));
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      job1.waitForCompletion(true);
  }
}

