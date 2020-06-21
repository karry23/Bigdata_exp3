package exp3;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.mapred.RecordWriter;
//import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.webapp.JobsBlock;
import org.apache.http.impl.conn.Wire;

public class PageRankViewer {
  public static class PageRankViewerMapper extends
      Mapper<LongWritable, Text, DoubleWritable, Text> {
    private Text outPage = new Text();
    private DoubleWritable outPr = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      String page = line[0];
      double pr = Double.parseDouble(line[1]);
      outPage.set(page);
      outPr.set(pr);
      context.write(outPr, outPage);
    }
  }

  public static class DescFloatComparator extends DoubleWritable.Comparator {
    // @Override
    public float compare(WritableComparator a,
        WritableComparable<DoubleWritable> b) {
      return -super.compare(a, b);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  public static class outputFormat extends FileOutputFormat<DoubleWritable, Text>{
	  
	  public RecordWriter<DoubleWritable, Text> getRecordWriter(
	            TaskAttemptContext job) throws IOException, InterruptedException {
	        // 得到文件输出的目录
	        Path fileDir = FileOutputFormat.getOutputPath(job);
	        Path fileName = new Path(fileDir.toString()+"outcome");
//	        System.out.println(fileName.getName());
	        Configuration conf = job.getConfiguration();
	        FSDataOutputStream file = fileName.getFileSystem(conf).create(fileName);
	        return new CustomRecordWrite(file);
	    }
  }
  public static class CustomRecordWrite extends RecordWriter<DoubleWritable, Text>{
	  private PrintWriter wPrintWriter = null;
	  public CustomRecordWrite(FSDataOutputStream file) {
	        this.wPrintWriter = new PrintWriter(file);
	    }
	  public void write(DoubleWritable key,Text value) 
			  throws IOException{
		  DecimalFormat decimalFormat=new DecimalFormat("0.0000000000");
		  
		  wPrintWriter.println("("+value.toString()+","+decimalFormat.format(key.get())+")");
//		  wPrintWriter.println("("+value.toString()+","+key.get()+")");
	  }
	@Override
	public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		wPrintWriter.close();
	}
  }
  
  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", "hdfs://localhost:9000");
      Job job3 = Job.getInstance(conf, "PageRankViewer");
      job3.setJarByClass(PageRankViewer.class);
      job3.setOutputKeyClass(DoubleWritable.class);
      job3.setSortComparatorClass(DescFloatComparator.class);
      job3.setOutputValueClass(Text.class);
      job3.setMapperClass(PageRankViewerMapper.class);
      job3.setOutputFormatClass(outputFormat.class);
      FileInputFormat.addInputPath(job3, new Path(args[0]));
      FileOutputFormat.setOutputPath(job3, new Path(args[1]));
      job3.waitForCompletion(true);
  }
}

