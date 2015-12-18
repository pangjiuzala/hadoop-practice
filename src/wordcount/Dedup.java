package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




/*Remove duplicate data*/

/**
 * @author liuxing
 *
 */
public class Dedup {
	static final String INPUT_PATH = "hdfs://master:9000/users/root/input";
	static final String OUT_PATH = "hdfs://master:9000/users/root/output";

	// map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
	}

	// reduce将输入中的key复制到输出数据的key上，并直接输出
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		final Job job = new Job(new Configuration(),
				Dedup.class.getSimpleName());
		@SuppressWarnings("unused")
		JobConf conf=new JobConf();
		
		// 打JAR包
		job.setJarByClass(Dedup.class);
		// 1.1 指定输入文件路径
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定哪个类用来格式化输入文件
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2指定自定义的Mapper类
		job.setMapperClass(Map.class);
		// 指定输出<k2,v2>的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 1.3 指定分区类
		// 1.4 TODO 排序、分区
		// 1.5 TODO （可选）合并
		job.setCombinerClass(Reduce.class);
		// 2.2 指定自定义的reduce类
		job.setReducerClass(Reduce.class);
		// 指定输出<k3,v3>的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 2.3 指定输出到哪里
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		// 设定输出文件的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把代码提交给JobTracker执行
		job.waitForCompletion(true);
	}
}