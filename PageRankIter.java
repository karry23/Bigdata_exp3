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

public class PageRankIter {
	private static final double damping = 0.85;

	public static class PRIterMapper extends
	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String pageKey = tuple[0];//网页名
			double pr = Double.parseDouble(tuple[1]);//获得当前网页的pr值
			//得到两种输出格式
			if (tuple.length > 2) {
				String[] linkPages = tuple[2].split(",");
				for (String linkPage : linkPages) {
					//若有v->u
					//则输出键值对<u,v \t pr(v)/v出度>
					//相当于获得了一份每个点的入边信息
					String prValue =
							pageKey + "\t" + String.valueOf(pr / linkPages.length);
					context.write(new Text(linkPage), new Text(prValue));
				}
				//保留一份出边列表到reduce中，保证迭代过程中的输出格式一样
				//"|"符号用来标识这是一份出边列表
				context.write(new Text(pageKey), new Text("|" + tuple[2]));
			}
		}
	}

	public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String links = "";
			double pagerank = 0;
			for (Text value : values) {
				String tmp = value.toString();
				//这是刚刚保存下来的出边列表
				if (tmp.startsWith("|")) {
					links = "\t" + tmp.substring(tmp.indexOf("|") + 1);// index从0开始
					continue;
				}

				String[] tuple = tmp.split("\t");
				//统计入边的pr值之和
				if (tuple.length > 1)
					pagerank += Double.parseDouble(tuple[1]);
			}
			pagerank = (double) (1 - damping) + damping * pagerank;
			//保持每次迭代中输入输出格式相同，其中pr值被更新，其他没变
			context.write(new Text(key), new Text(String.valueOf(pagerank) + links));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		Job job2 = Job.getInstance(conf, "PageRankIter");
		job2.setJarByClass(PageRankIter.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(PRIterMapper.class);
		job2.setReducerClass(PRIterReducer.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.waitForCompletion(true);
	}
}
