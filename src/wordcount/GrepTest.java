package wordcount;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*Use MapReduce program to simulates grep command*/
/**
 * @author liuxing
 *
 */
public class GrepTest {
	public static class grepMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {

		private String pattern[] = { "2012-3-3","c"  };// insert word you want to find

		public void configure(JobConf job) {

			// debugging
			System.out
					.println("Inside configure function printing elements of list: ");

			Scanner scan;
			try {
				scan = new Scanner(new File(job.get("patternFile")));
				LinkedList<String> list = new LinkedList<String>();
				while (scan.hasNext()) {
					list.add(scan.nextLine());

					// debugging
					System.out.println(list.peekLast());
				}
				scan.close();
				pattern = new String[list.size()];
				list.toArray(pattern);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// debugging
			System.out
					.println("Inside configure function printing elements of pattern[]: ");
			for (int i = 0; i < pattern.length; i++) {
				System.out.println(pattern[i]);
			}

		}

		public void map(LongWritable key, Text values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			// debugging
			System.out.println("Inside map() function : ");
			System.out.println("values =\t  " + values.toString() + "\n");

			for (int i = 0; i < pattern.length; i++) {
				if (values.toString().contains(pattern[i])) {
					// debugging
					System.out.println("*********VALUE MATCHED**********");

					output.collect(key, values);
					break;
				}
			}
		}
	}

	public static class grepReducer extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			// replace KeyType with the real type of your key
			output.collect(key, values.next());
			// process value
		}
	}

	public static void main(String[] args) {

		JobClient client = new JobClient();
		JobConf conf = new JobConf(GrepTest.class);

		conf.set("patternFile", "hdfs://master:9000/users/root/output");

		// TODO: specify output types
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		// TODO: specify a mapper
		conf.setMapperClass(grepMapper.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// TODO: specify a reducer
		conf.setReducerClass(grepReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
