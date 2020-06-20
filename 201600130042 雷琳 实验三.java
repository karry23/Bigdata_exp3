package CH8;
/************************************GraphBuilder******************************************/
/*
 * 建立链接关系图
 */
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {
    // 正则表达式，匹配出一对方括号”[]“及其所包含的内容，注意方括号内的内容不含换行符并且至少含有一个字符，寻找连接网页 
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException { 
      
      context.write(new Text(value.toString().split("\t")[0]), new Text( "1.0\t" + value.toString().split("\t")[1]));
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
	  Job job1 = new Job(conf, "GraphBuilder");
      job1.setJarByClass(GraphBuilder.class);
      job1.setJar("PageRank.jar");
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      job1.setMapperClass(GraphBuilderMapper.class);
      job1.setReducerClass(GraphBuilderReducer.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      job1.waitForCompletion(true);
  }
}
/************************************PageRankIter ******************************************/
 
import java.io.IOException;
/*
 * 迭代计算pagerank的值，直到满足运算结束条件
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PageRankIter {
  private static final double damping = 0.85;
  public static class PRIterMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tuple = line.split("\t");
      String pageKey = tuple[0];
      double pr = Double.parseDouble(tuple[1]);
      if (tuple.length > 2) {
        String[] linkPages = tuple[2].split(",");
        for (String linkPage : linkPages) {
          String prValue =  pageKey + "\t" + String.valueOf(pr / linkPages.length);
          context.write(new Text(linkPage), new Text(prValue));
        }
        context.write(new Text(pageKey), new Text("|" + tuple[2]));//传递链表
      }
    }
  }
  public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
      String links = "";
      double pagerank = 0;
      for (Text value : values) 
      {
        String tmp = value.toString();
        if (tmp.startsWith("|")) {
          links = "\t" + tmp.substring(tmp.indexOf("|") + 1);// index从0开始
          continue;
        }
        String[] tuple = tmp.split("\t");
        if (tuple.length > 1)
        	//对所有指向该网页的PR值进行求和
          pagerank += Double.parseDouble(tuple[1]);
      }
      pagerank = (double) (1 - damping) + damping * pagerank; // PageRank的计算迭代公式
      context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job2 = new Job(conf, "PageRankIter");
    job2.setJarByClass(PageRankIter.class);
    job2.setJar("PageRank.jar");
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setMapperClass(PRIterMapper.class);
    job2.setReducerClass(PRIterReducer.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job2.waitForCompletion(true);
  }
}
/************************************PageRankViewer ******************************************/
 
/*
 * 排序输出，从大到小
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import CH8.PageRankIter.PRIterReducer;
public class PageRankViewer {
  public static class PageRankViewerMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private Text outPage = new Text();
    private FloatWritable outPr = new FloatWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      String page = line[0];
      float pr = Float.parseFloat(line[1]);
      outPage.set(page);
      outPr.set(pr);
      context.write(outPr, outPage);
    }
  }
  /**由于最后输出的不是按照小-大，所以需要重载key的比较函数**/
  public static class DescFloatComparator extends FloatWritable.Comparator {
    // @Override
    public float compare(WritableComparator a, WritableComparable<FloatWritable> b) {
      return -super.compare(a, b);
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }
  
  /*交换key value, 并格式化输出*/
	public static class PageRankViewerReducer extends Reducer<FloatWritable, Text, Text, Text> {
		public void reduce(FloatWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			double pr = key.get();
			String page = "";
			for(Text t : value) {
				page += t.toString();
			}
			context.write(new Text("("+page), new Text(String.format("%.10f", pr)+")"));
		}
	}
  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job3 = new Job(conf, "PageRankViewer");
      job3.setJarByClass(PageRankViewer.class);
      job3.setJar("PageRank.jar");
      conf.set("mapred.textoutputformat.ignoreseparator","true"); 
      conf.set("mapred.textoutputformat.separator",",");
      
      
      job3.setOutputKeyClass(FloatWritable.class);
      job3.setSortComparatorClass(DescFloatComparator.class);
      job3.setOutputValueClass(Text.class);
      
      job3.setMapperClass(PageRankViewerMapper.class);
      job3.setReducerClass(PageRankViewerReducer.class);
      
      FileInputFormat.addInputPath(job3, new Path(args[0]));
      FileOutputFormat.setOutputPath(job3, new Path(args[1]));
      job3.waitForCompletion(true);
  }
}
/************************************PageRankDriver ******************************************/
/*
 * 串接起来
 */
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import CH8.GraphBuilder;
import CH8.PageRankIter;
import CH8.PageRankViewer;

public class PageRankDriver {
  private static int times = 10; // 设置迭代次数

  public static void main(String[] args) throws Exception {  
	  Configuration conf = new Configuration();
	  Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}
		
    String[] forGB = { "", args[1] + "/Data0" };
    forGB[0] = args[0];
    GraphBuilder.main(forGB);//初始化图 

    String[] forItr = { "", "" };//每次的输入目录为上一次的输出目录
    for (int i = 0; i < times; i++) {
      forItr[0] = args[1] + "/Data" + i;
      forItr[1] = args[1] + "/Data" + String.valueOf(i + 1);
     // System.out.print(i);
      PageRankIter.main(forItr);
    }

    String[] forRV = { args[1] + "/Data" + times, args[1] + "/FinalRank" };
    PageRankViewer.main(forRV);
  }
}

/********************************************************************************************************
*********************************************Scala*******************************************************
********************************************************************************************************/

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io._;


val hdfs = FileSystem.get(URI.create("hdfs://master:9000/Experiment_3/DataSet"), new Configuration)
var fp : FSDataInputStream = hdfs.open(new Path("hdfs://master:9000/Experiment_3/DataSet"))
var isr : InputStreamReader = new InputStreamReader(fp)
var bReader : BufferedReader = new BufferedReader(isr)
var line:String = bReader.readLine();
var i=0;
var t = List(("",List("a")));

while(line!=null) {
    var a = line.split("\t");//From
    var b = a(1).split(",");//To
    t = t ++ List((a(0),b.toList));//From [TO1 To2 ...]
    line = bReader.readLine();
}

isr.close();
bReader.close();
t = t.drop(1)
val links = sc.parallelize(t).persist();//RDD缓存cache
var ranks=links.mapValues(v=>1.0)//初始化pr=1
 
for (i <- 0 until 10) {
	val contributions=links.join(ranks).flatMap {
		case (pageId,(links,rank)) => links.map(dest=>(dest,rank/links.size))//iter的map
	}
		ranks=contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)//iter的reduce
}
val output = ranks.collect.sortWith{
      case (a,b)=>{
        a._2>b._2
      }
    }


val path = new Path("hdfs://master:9000/user/201600130042/pagerank_spark_output");
val oos = new PrintWriter(hdfs.create(path)) ;
for(i<-0 to output.length-1){
    oos.write("("+output(i)._1 + "," +"%11.10f".format(output(i)._2)+")\n");
}
oos.close();

